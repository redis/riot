package com.redislabs.riot.batch.redis.writer;

import java.util.List;

import com.redislabs.riot.batch.redis.LettuceConnector;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;

public class LettuceSyncItemWriter<C extends StatefulConnection<String, String>, R extends BaseRedisCommands<String, String>, O>
		extends AbstractLettuceItemWriter<C, R, O> {

	public LettuceSyncItemWriter(LettuceConnector<C, R> connector) {
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
