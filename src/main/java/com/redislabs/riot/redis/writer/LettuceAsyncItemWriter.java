package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Builder;
import lombok.Setter;

public class LettuceAsyncItemWriter<O> extends AbstractLettuceItemWriter<O> {

	private @Setter long timeout;
	private @Setter boolean fireAndForget;

	@SuppressWarnings("rawtypes")
	@Builder
	protected LettuceAsyncItemWriter(CommandWriter<O> writer,
			GenericObjectPool<? extends StatefulConnection<String, String>> pool, Function api, long timeout,
			boolean fireAndForget) {
		super(writer, pool, api);
		this.timeout = timeout;
		this.fireAndForget = fireAndForget;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void write(List<? extends O> items, Object commands) {
		BaseRedisAsyncCommands<String, String> asyncCommands = ((BaseRedisAsyncCommands<String, String>) commands);
		asyncCommands.setAutoFlushCommands(false);
		List<RedisFuture> futures = new ArrayList<>();
		for (O item : items) {
			try {
				futures.add((RedisFuture) writer.write(asyncCommands, item));
			} catch (Exception e) {
				logWriteError(item, e);
			}
		}
		asyncCommands.flushCommands();
		if (fireAndForget) {
			return;
		}
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
