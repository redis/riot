package com.redislabs.riot.batch.redis.writer;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.api.StatefulConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public abstract class AbstractLettuceItemWriter<C extends StatefulConnection<String, String>, R, O>
		extends AbstractRedisItemWriter<R, O> {

	@Setter
	private C connection;
	@Setter
	private Function api;

	@Override
	public void write(List<? extends O> items) throws Exception {
		R commands = (R) api.apply(connection);
		write(items, commands);
	}

	protected abstract void write(List<? extends O> items, R commands);

}
