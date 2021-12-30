package com.redis.riot;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.redis.spring.batch.support.PollableItemReader;

public class SynchronizedPollableItemReader<T> implements PollableItemReader<T>, InitializingBean {

	private PollableItemReader<T> delegate;

	/**
	 * This delegates to the read method of the <code>delegate</code>
	 */
	@Nullable
	public synchronized T read()
			throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return this.delegate.read();
	}

	public void close() {
		this.delegate.close();
	}

	public void open(ExecutionContext executionContext) {
		this.delegate.open(executionContext);
	}

	public void update(ExecutionContext executionContext) {
		this.delegate.update(executionContext);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.delegate, "A delegate item reader is required");
	}

	public void setDelegate(PollableItemReader<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws Exception {
		return delegate.poll(timeout, unit);
	}
}
