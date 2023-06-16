package com.redis.riot.cli.common;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

public class NoopItemWriter<T> implements ItemWriter<T> {

	@Override
	public void write(List<? extends T> items) throws Exception {
		// Do nothing
	}

}
