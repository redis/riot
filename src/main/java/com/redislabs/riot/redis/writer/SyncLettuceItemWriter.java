package com.redislabs.riot.redis.writer;

import java.util.List;

public class SyncLettuceItemWriter<O> extends AbstractLettuceItemWriter<O> {

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
