package com.redislabs.riot.transfer;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class CappedReader<I> implements ItemStreamReader<I> {

	private Object lock = new Object();
	private ItemReader<I> reader;
	private AtomicLong currentItemCount = new AtomicLong();
	private long maxItemCount;

	public CappedReader(ItemReader<I> reader, long maxItemCount) {
		this.reader = reader;
		this.maxItemCount = maxItemCount;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (reader instanceof ItemStreamReader) {
			((ItemStreamReader<I>) reader).open(executionContext);
		}
	}

	@Override
	public I read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		synchronized (lock) {
			if (currentItemCount.getAndIncrement() < maxItemCount) {
				return reader.read();
			}
			return null;
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (reader instanceof ItemStreamReader) {
			((ItemStreamReader<I>) reader).close();
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (reader instanceof ItemStreamReader) {
			((ItemStreamReader<I>) reader).update(executionContext);
		}
	}
}
