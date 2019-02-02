
package com.redislabs.recharge.writer.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public abstract class AbstractPipelineRedisWriter extends AbstractRedisWriter {

	RediSearchAsyncCommands<String, String> commands;

	protected AbstractPipelineRedisWriter(RediSearchClient client, RedisWriterConfiguration config) {
		super(client, config);
	}

	@Override
	protected void open(StatefulRediSearchConnection<String, String> connection) {
		commands = connection.async();
		commands.setAutoFlushCommands(false);
		doOpen();
	}

	protected void doOpen() {
	}

	@Override
	public synchronized void close() {
		commands = null;
		super.close();
	}

	@Override
	public void write(List<? extends Map> records) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		super.write(records);
		commands.flushCommands();
		boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS, futures.toArray(new RedisFuture[futures.size()]));
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
	}

}
