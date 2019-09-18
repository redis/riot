package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public LettuceItemWriter(AbstractRedisClient client, Supplier<ClientResources> resources, GenericObjectPool<S> pool,
			RedisMapWriter writer, Function<S, C> async) {
		this.client = client;
		this.resources = resources;
		this.pool = pool;
		this.writer = writer;
		this.async = async;
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		S connection = pool.borrowObject();
		List<RedisFuture<?>> futures = new ArrayList<>();
		try {
			C commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			for (Map<String, Object> item : items) {
				futures.add(writer.write(commands, item));
			}
			commands.flushCommands();
			for (int index = 0; index < futures.size(); index++) {
				RedisFuture<?> future = futures.get(index);
				try {
					future.get(1, TimeUnit.SECONDS);
				} catch (Exception e) {
					log.error("Could not write record {}: {}", items.get(index), future.getError());
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
