package com.redislabs.riot.batch;

import org.springframework.batch.item.ItemReader;

public class ThrottlingItemReader<T> implements ItemReader<T> {

	private ItemReader<T> reader;
	private long sleep;

	public ThrottlingItemReader(ItemReader<T> reader, long sleep) {
		this.reader = reader;
		this.sleep = sleep;
	}

	@Override
	public T read() throws Exception {
		Thread.sleep(sleep);
		return reader.read();
	}

}
