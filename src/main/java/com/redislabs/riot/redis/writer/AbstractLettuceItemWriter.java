package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public abstract class AbstractLettuceItemWriter<O> extends AbstractRedisItemWriter<O> {

	private @Setter GenericObjectPool<? extends StatefulConnection<String, String>> pool;
	private @Setter Function api;

	@Override
	public void write(List<? extends O> items) throws Exception {
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
			Object commands = api.apply(connection);
			write(items, commands);
		}
	}

	protected abstract void write(List<? extends O> items, Object commands);

}
