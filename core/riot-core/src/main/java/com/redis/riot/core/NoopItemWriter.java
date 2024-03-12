package com.redis.riot.core;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

public class NoopItemWriter<T> implements ItemWriter<T> {

	@Override
	public void write(Chunk<? extends T> items) {
		// Do nothing
	}

}
