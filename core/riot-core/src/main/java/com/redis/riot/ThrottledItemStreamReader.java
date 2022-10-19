package com.redis.riot;

import java.time.Duration;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;

public class ThrottledItemStreamReader<T> extends AbstractItemStreamItemReader<T> {

	private final ItemReader<T> delegate;
	private final long sleep;

	public ThrottledItemStreamReader(ItemReader<T> delegate, Duration sleepDuration) {
		Assert.notNull(delegate, "Reader delegate must not be null");
		Assert.notNull(sleepDuration, "Sleep duration must not be null");
		Assert.isTrue(!sleepDuration.isNegative() && !sleepDuration.isZero(),
				"Sleep duration must be strictly positive");
		this.delegate = delegate;
		this.sleep = sleepDuration.toMillis();
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		((ItemStream) delegate).open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) {
		((ItemStream) delegate).update(executionContext);
	}

	@Override
	public void close() {
		((ItemStream) delegate).close();
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		if (delegate instanceof ItemStreamSupport) {
			((ItemStreamSupport) delegate).setName(name);
		}
	}

	@Override
	public T read() throws Exception {
		Thread.sleep(sleep);
		return delegate.read();
	}
}
