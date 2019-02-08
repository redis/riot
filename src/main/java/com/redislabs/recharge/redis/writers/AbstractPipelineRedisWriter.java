
package com.redislabs.recharge.redis.writers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
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

	protected ThreadLocal<RediSearchAsyncCommands<String, String>> commands = new ThreadLocal<>();

	@Override
	public void open(ExecutionContext executionContext) {
		RediSearchAsyncCommands<String, String> async = connection.async();
		async.setAutoFlushCommands(false);
		commands.set(async);
		super.open(executionContext);
	}

	@Override
	public void write(List<? extends Map> records) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		super.write(records);
		commands.get().flushCommands();
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

	@Override
	protected void write(String key, Map record) {
		write(key, record, commands.get());
	}

	protected abstract void write(String key, Map record, RediSearchAsyncCommands<String, String> commands);

}
