package com.redislabs.riot.redis.writer;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;

public class SyncLettuceItemWriter<C extends StatefulConnection<String, String>, O>
		extends AbstractLettuceItemWriter<C, O> {

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
