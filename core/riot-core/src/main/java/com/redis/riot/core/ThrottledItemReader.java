package com.redis.riot.core;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.DelegatingItemStreamSupport;
import com.redis.spring.batch.reader.PollableItemReader;

public class ThrottledItemReader<T> extends DelegatingItemStreamSupport
		implements ItemStreamReader<T>, PollableItemReader<T> {

	private final ItemReader<T> delegate;
	private final long sleep;

	public ThrottledItemReader(ItemReader<T> delegate, Duration sleepDuration) {
		super(delegate);
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(delegate, "Reader delegate must not be null");
		Assert.notNull(sleepDuration, "Sleep duration must not be null");
		Assert.isTrue(!sleepDuration.isNegative() && !sleepDuration.isZero(),
				"Sleep duration must be strictly positive");
		this.delegate = delegate;
		this.sleep = sleepDuration.toMillis();
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
		sleep();
		return delegate.read();
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		sleep();
		return ((PollableItemReader<T>) delegate).poll(timeout, unit);
	}

	private void sleep() throws InterruptedException {
		Thread.sleep(sleep);
	}

}
