package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class SynchronizedListItemWriter<T> extends AbstractItemStreamItemWriter<T> {

	private List<T> writtenItems = new ArrayList<>();

	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		writtenItems.addAll(items);
	}

	public List<? extends T> getWrittenItems() {
		return this.writtenItems;
	}

}