package com.redislabs.riot.redis.writer;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;

public class SyncLettuceItemWriter<C extends StatefulConnection<String, String>, R extends BaseRedisCommands<String, String>, O>
		extends AbstractLettuceItemWriter<C, R, O> {

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
