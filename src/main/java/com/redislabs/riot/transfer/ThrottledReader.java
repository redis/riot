package com.redislabs.riot.transfer;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Builder
public @Data class ThrottledReader<I> implements ItemStreamReader<I> {

	private ItemReader<I> reader;
	private long sleep;

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (reader instanceof ItemStreamReader) {
			((ItemStreamReader<I>) reader).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (reader instanceof ItemStreamReader) {
			((ItemStreamReader<I>) reader).update(executionContext);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (reader instanceof ItemStreamReader) {
			((ItemStreamReader<I>) reader).close();
		}
	}

	@Override
	public I read() throws Exception {
		Thread.sleep(sleep);
		return reader.read();
	}

}
