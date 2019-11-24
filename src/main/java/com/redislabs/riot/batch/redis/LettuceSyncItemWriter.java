package com.redislabs.riot.batch.redis;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;

public class LettuceSyncItemWriter<K, V, C extends StatefulConnection<K, V>, R extends BaseRedisCommands<K, V>, O>
		extends AbstractLettuceItemWriter<K, V, C, R, O> {

	public LettuceSyncItemWriter(LettuceConnector<K, V, C, R> connector) {
		super(connector);
	}

	@Override
	protected void write(List<? extends O> items, R commands) {
		for (O item : items) {
			try {
				writer.write(commands, item);
			} catch (Exception e) {
				logWriteError(item, e);
			}
		}
	}

}
