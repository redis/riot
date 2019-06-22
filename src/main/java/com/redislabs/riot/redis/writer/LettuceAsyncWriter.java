package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LettuceAsyncWriter<T extends StatefulRedisConnection<String, String>, C extends RedisAsyncCommands<String, String>>
		extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private GenericObjectPool<T> pool;
	private LettuceItemWriter<C> writer;

	public LettuceAsyncWriter(GenericObjectPool<T> pool, LettuceItemWriter<C> writer) {
		setName(ClassUtils.getShortName(LettuceAsyncWriter.class));
		this.pool = pool;
		this.writer = writer;
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		List<RedisFuture<?>> futures = new ArrayList<>();
		if (pool.isClosed()) {
			return;
		}
		T connection = pool.borrowObject();
		try {
			@SuppressWarnings("unchecked")
			C commands = (C) connection.async();
			commands.setAutoFlushCommands(false);
			for (Map<String, Object> item : items) {
				RedisFuture<?> future = writer.write(commands, item);
				if (future != null) {
					futures.add(future);
				}
			}
			commands.flushCommands();
			try {
				boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
						futures.toArray(new RedisFuture[futures.size()]));
				if (result) {
					log.debug("Wrote {} records", items.size());
				} else {
					log.warn("Could not write {} records", items.size());
					for (RedisFuture<?> future : futures) {
						if (future.getError() != null) {
							log.error(future.getError());
						}
					}
				}
			} catch (RedisCommandExecutionException e) {
				log.error("Could not execute commands", e);
			}
		} finally {
			pool.returnObject(connection);
		}
	}

	@Override
	public void close() {
		pool.close();
		super.close();
	}
}
