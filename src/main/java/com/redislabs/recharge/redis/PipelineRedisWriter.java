
package com.redislabs.recharge.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public abstract class PipelineRedisWriter extends RedisWriter {

	@Override
	public void write(List<? extends Map> records) throws Exception {
		List<RedisFuture<?>> futures = new ArrayList<>();
		if (pool.isClosed()) {
			return;
		}
		StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
		try {
			RediSearchAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			for (Map record : records) {
				RedisFuture<?> future = write(record, commands);
				if (future != null) {
					futures.add(future);
				}
			}
			commands.flushCommands();
			try {
				boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
						futures.toArray(new RedisFuture[futures.size()]));
				if (result) {
					log.debug("Wrote {} records", records.size());
				} else {
					log.warn("Could not write {} records", records.size());
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

	protected abstract RedisFuture<?> write(Map record, RediSearchAsyncCommands<String, String> commands);

}
