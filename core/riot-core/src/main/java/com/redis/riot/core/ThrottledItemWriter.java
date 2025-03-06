package com.redis.riot.core;

import java.time.Duration;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

public class ThrottledItemWriter<T> implements ItemStreamWriter<T> {

	private final ItemWriter<T> delegate;
	private final Duration sleep;

	public ThrottledItemWriter(ItemWriter<T> delegate, Duration sleep) {
		Assert.notNull(delegate, "Delegate must not be null");
		Assert.notNull(sleep, "Sleep must not be null");
		Assert.isTrue(sleep.isPositive(), "Sleep duration must be positive");
		this.delegate = delegate;
		this.sleep = sleep;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).close();
		}
	}

	@Override
	public void write(Chunk<? extends T> items) throws Exception {
		delegate.write(items);
		Thread.sleep(sleep);
	}

}
