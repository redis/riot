package com.redis.riot.core;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.DelegatingItemStreamSupport;

public class ThrottledItemWriter<T> extends DelegatingItemStreamSupport implements ItemStreamWriter<T> {

	private final ItemWriter<T> delegate;
	private final long sleep;

	public ThrottledItemWriter(ItemWriter<T> delegate, Duration sleepDuration) {
		super(delegate);
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(delegate, "Delegate must not be null");
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
	public void write(List<? extends T> items) throws Exception {
		delegate.write(items);
		Thread.sleep(sleep);
	}

}
