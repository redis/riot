
package com.redislabs.recharge.redis.writers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.AbstractRedisWriterConfiguration;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public abstract class AbstractPipelineRedisWriter<T extends AbstractRedisWriterConfiguration>
		extends AbstractRedisWriter<T> {

	protected AbstractPipelineRedisWriter(T config) {
		super(config);
	}

	@Override
	public void write(List<? extends Map> records) throws Exception {
		List<RedisFuture<?>> futures = new ArrayList<>();
		StatefulRediSearchConnection<String, String> connection = getConnection();
		try {
			RediSearchAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			records.forEach(record -> write(getKey(record), record, commands));
			commands.flushCommands();
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
		} finally {
			release(connection);
		}
	}

	protected abstract void write(String key, Map record, RediSearchAsyncCommands<String, String> commands);

}
