package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.riot.redis.writer.RedisMapWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.resource.ClientResources;

public class LettuceItemWriter<S extends StatefulConnection<String, String>, C extends BaseRedisAsyncCommands<String, String>>
		extends AbstractRedisItemWriter {

	private final Logger log = LoggerFactory.getLogger(LettuceItemWriter.class);

	private AbstractRedisClient client;
	private Supplier<ClientResources> resources;
	private GenericObjectPool<S> pool;
	private RedisMapWriter writer;
	private Function<S, C> async;
	private long timeout;

	public LettuceItemWriter(AbstractRedisClient client, Supplier<ClientResources> resources, GenericObjectPool<S> pool,
			RedisMapWriter writer, Function<S, C> async, long timeout) {
		this.client = client;
		this.resources = resources;
		this.pool = pool;
		this.writer = writer;
		this.async = async;
		this.timeout = timeout;
	}

	@Override
	protected void doWrite(List<? extends Map<String, Object>> items) throws Exception {
		S connection = pool.borrowObject();
		List<RedisFuture<?>> futures = new ArrayList<>();
		try {
			C commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			items.forEach(item -> futures.add(writer.write(commands, item)));
			commands.flushCommands();
			for (int index = 0; index < futures.size(); index++) {
				RedisFuture<?> future = futures.get(index);
				if (future == null) {
					continue;
				}
				try {
					future.get(timeout, TimeUnit.SECONDS);
				} catch (Exception e) {
					log.error("Could not write record {}", items.get(index), e);
				}
			}
		} finally {
			pool.returnObject(connection);
		}
	}

	@Override
	public synchronized void close() {
		// Take care of multi-threaded writer by only closing on the last call
		if (pool != null && !hasActiveThreads()) {
			log.debug("Closing pool");
			pool.close();
			pool = null;
			client.shutdown();
			resources.get().shutdown();
		}
		super.close();
	}

}
