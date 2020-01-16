package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractLettuceItemWriter<C extends StatefulConnection<String, String>, R, O>
		extends AbstractRedisItemWriter<R, O> {

	@Setter
	private GenericObjectPool<C> pool;
	@Setter
	private Function api;

	@Override
	public void write(List<? extends O> items) throws Exception {
		try (C connection = pool.borrowObject()) {
			R commands = (R) api.apply(connection);
			write(items, commands);
		}
	}

	protected abstract void write(List<? extends O> items, R commands);

}
