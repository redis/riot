package com.redislabs.riot;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class ThrottledItemStreamReader<T> implements ItemStreamReader<T> {

	private ItemStreamReader<T> reader;
	private long sleep;

	public ThrottledItemStreamReader(ItemStreamReader<T> reader, long sleep) {
		this.reader = reader;
		this.sleep = sleep;
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

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		Thread.sleep(sleep);
		return reader.read();
	}

}
