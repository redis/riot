package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncLettuceItemWriter<C extends StatefulConnection<String, String>, R extends BaseRedisAsyncCommands<String, String>, O>
		extends AbstractLettuceItemWriter<C, R, O> {

	@Setter
	private long timeout;

	@SuppressWarnings("rawtypes")
	@Override
	protected void write(List<? extends O> items, R commands) {
		commands.setAutoFlushCommands(false);
		List<RedisFuture> futures = new ArrayList<>();
		for (O item : items) {
			try {
				futures.add((RedisFuture) writer.write(commands, item));
			} catch (Exception e) {
				logWriteError(item, e);
			}
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
					log.error("Could not write record", e);
				}
			}
		}
	}

}
