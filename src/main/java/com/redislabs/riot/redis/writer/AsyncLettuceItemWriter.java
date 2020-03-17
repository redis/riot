package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class AsyncLettuceItemWriter<O> extends AbstractLettuceItemWriter<O> {

	@Setter
	private long timeout;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void write(List<? extends O> items, Object commands) {
		((BaseRedisAsyncCommands<String, String>) commands).setAutoFlushCommands(false);
		List<RedisFuture> futures = new ArrayList<>();
		for (O item : items) {
			try {
				futures.add((RedisFuture) writer.write(commands, item));
			} catch (Exception e) {
				logWriteError(item, e);
			}
		}
		((BaseRedisAsyncCommands<String, String>) commands).flushCommands();
		for (int index = 0; index < futures.size(); index++) {
			RedisFuture future = futures.get(index);
			if (future == null) {
				continue;
			}
			try {
				future.get(timeout, TimeUnit.SECONDS);
			} catch (Exception e) {
				logWriteError(items.get(index), e);
			}
		}
	}

}
