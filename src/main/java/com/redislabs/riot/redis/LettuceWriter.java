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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.resource.ClientResources;

public class LettuceWriter<S extends StatefulConnection<String, String>, C extends BaseRedisAsyncCommands<String, String>>
		extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(LettuceWriter.class);

	private AbstractRedisClient client;
	private Supplier<ClientResources> resources;
	private GenericObjectPool<S> pool;
	private LettuceItemWriter<C> writer;
	private Function<S, C> async;

	public LettuceWriter(AbstractRedisClient client, Supplier<ClientResources> resources, GenericObjectPool<S> pool,
			LettuceItemWriter<C> writer, Function<S, C> async) {
		setName(ClassUtils.getShortName(this.getClass()));
		this.client = client;
		this.resources = resources;
		this.pool = pool;
		this.writer = writer;
		this.async = async;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		S connection = pool.borrowObject();
		List<RedisFuture<?>> futures = new ArrayList<>();
		try {
			C commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			for (Map<String, Object> item : items) {
				RedisFuture<?> future = writer.write(commands, item);
				if (future != null) {
					futures.add(future);
				}
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
	public void close() {
		// Take care of multi-threaded writer by only closing on the last call
		if (pool != null && pool.getNumActive() == 0) {
			log.debug("Closing pool");
			pool.close();
			pool = null;
			client.shutdown();
			resources.get().shutdown();
		}
		super.close();
	}

}
