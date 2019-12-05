package com.redislabs.riot.batch.redis.writer;

import java.util.List;

import com.redislabs.riot.batch.redis.LettuceConnector;

import io.lettuce.core.api.StatefulConnection;

public abstract class AbstractLettuceItemWriter<C extends StatefulConnection<String, String>, R, O>
		extends AbstractRedisItemWriter<R, O> {

	private LettuceConnector<C, R> connector;

	public AbstractLettuceItemWriter(LettuceConnector<C, R> connector) {
		this.connector = connector;
	}

	@Override
	public void write(List<? extends O> items) throws Exception {
		C connection = connector.pool().borrowObject();
		R commands = connector.api().apply(connection);
		try {
			write(items, commands);
		} finally {
			connector.pool().returnObject(connection);
		}
	}

	protected abstract void write(List<? extends O> items, R commands);

}
