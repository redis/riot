package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import lombok.Builder;

public class LettuceSyncItemWriter<O> extends AbstractLettuceItemWriter<O> {

	@SuppressWarnings("rawtypes")
	@Builder
	protected LettuceSyncItemWriter(CommandWriter<O> writer,
			GenericObjectPool<? extends StatefulConnection<String, String>> pool, Function api) {
		super(writer, pool, api);
	}

	@Override
	protected void write(List<? extends O> items, Object commands) {
		for (O item : items) {
			try {
				writer.write(commands, item);
			} catch (Exception e) {
				logWriteError(item, e);
			}
		}
	}

}
