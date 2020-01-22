package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractLettuceItemWriter<C extends StatefulConnection<String, String>, O>
		extends AbstractRedisItemWriter<O> {

	private @Setter GenericObjectPool<C> pool;
	private @Setter Function api;

	@Override
	public void write(List<? extends O> items) throws Exception {
		try (C connection = pool.borrowObject()) {
			Object commands = api.apply(connection);
			write(items, commands);
		}
	}

	protected abstract void write(List<? extends O> items, Object commands);

}
