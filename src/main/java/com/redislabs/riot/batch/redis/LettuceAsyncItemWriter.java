package com.redislabs.riot.batch.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LettuceAsyncItemWriter<K, V, C extends StatefulConnection<K, V>, R extends BaseRedisAsyncCommands<K, V>, O>
		extends AbstractLettuceItemWriter<K, V, C, R, O> {

	private RedisWriter<R, O> writer;
	private long timeout;

	public LettuceAsyncItemWriter(LettuceConnector<K, V, C, R> connector, RedisWriter<R, O> writer, long timeout) {
		super(connector);
		this.writer = writer;
		this.timeout = timeout;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void write(List<? extends O> items, R commands) {
		List<RedisFuture> futures = new ArrayList<>();
		commands.setAutoFlushCommands(false);
		for (O item : items) {
			futures.add((RedisFuture) writer.write(commands, item));
		}
		commands.flushCommands();
		for (int index = 0; index < futures.size(); index++) {
			RedisFuture future = futures.get(index);
			if (future == null) {
				continue;
			}
			try {
				future.get(timeout, TimeUnit.SECONDS);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Could not write record {}", items.get(index), e);
				} else {
					log.error("Could not write record: {}", e.getMessage());
				}
			}
		}
	}

}
