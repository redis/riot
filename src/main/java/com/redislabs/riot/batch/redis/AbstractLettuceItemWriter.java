package com.redislabs.riot.batch.redis;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractLettuceItemWriter<K, V, C extends StatefulConnection<K, V>, R, O>
		extends AbstractRedisItemWriter<R, O> {

	private LettuceConnector<K, V, C, R> connector;

	public AbstractLettuceItemWriter(LettuceConnector<K, V, C, R> connector) {
		this.connector = connector;
	}

	@Override
	public void write(List<? extends O> items) throws Exception {
		C connection = connector.getPool().borrowObject();
		R commands = connector.getApi().apply(connection);
		try {
			write(items, commands);
		} finally {
			connector.getPool().returnObject(connection);
		}
	}

	protected abstract void write(List<? extends O> items, R commands);

	@Override
	public synchronized void close() {
		// Take care of multi-threaded writer by only closing on the last call
		if (connector.getPool() != null && !hasActiveThreads()) {
			log.debug("Closing pool");
			connector.getPool().close();
			connector.getClient().shutdown();
			connector.getResources().get().shutdown();
		}
		super.close();
	}

}
