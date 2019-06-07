package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LettuceWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	@Setter
	private GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;
	@Setter
	private RedisItemWriter itemWriter;

	public LettuceWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	public String getName() {
		return getExecutionContextKey("name");
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		List<RedisFuture<?>> futures = new ArrayList<>();
		if (pool.isClosed()) {
			return;
		}
		StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
		try {
			RediSearchAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			for (Map<String, Object> item : items) {
				RedisFuture<?> future = (RedisFuture<?>) itemWriter.write(commands, item);
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
