package com.redislabs.riot;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

public class ThrottlingItemStreamReader<T> extends ThrottlingItemReader<T> implements ItemStreamReader<T> {

	private ItemStreamReader<T> reader;

	public ThrottlingItemStreamReader(ItemStreamReader<T> reader, long sleep) {
		super(reader, sleep);
		this.reader = reader;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		reader.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		reader.update(executionContext);
	}

	@Override
	public void close() throws ItemStreamException {
		reader.close();
	}

}
