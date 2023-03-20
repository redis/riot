package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.common.Openable;

public class SynchronizedListItemWriter<T> extends AbstractItemStreamItemWriter<T> implements Openable {

	private List<T> writtenItems = new ArrayList<>();
	private boolean open;

	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		writtenItems.addAll(items);
	}

	public List<? extends T> getWrittenItems() {
		return this.writtenItems;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		this.open = true;
	}

	@Override
	public void close() {
		super.close();
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}
}