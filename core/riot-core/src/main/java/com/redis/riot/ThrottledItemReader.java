package com.redis.riot;

import java.time.Duration;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.util.Assert;

public class ThrottledItemReader<T> implements ItemReader<T> {

	private final ItemReader<T> delegate;
	private final long sleep;

	public ThrottledItemReader(ItemReader<T> delegate, Duration sleepDuration) {
		Assert.notNull(delegate, "Reader delegate must not be null");
		Assert.notNull(sleepDuration, "Sleep duration must not be null");
		Assert.isTrue(!sleepDuration.isNegative() && !sleepDuration.isZero(),
				"Sleep duration must be strictly positive");
		this.delegate = delegate;
		this.sleep = sleepDuration.toMillis();
	}

	@Override
	public T read() throws Exception {
		Thread.sleep(sleep);
		return delegate.read();
	}

	public static <T> ItemReader<T> create(ItemReader<T> reader, Duration duration) {
		if (duration.isZero()) {
			return reader;
		}
		if (reader instanceof ItemStream) {
			return new ThrottledItemStreamReader<>(reader, duration);
		}
		return new ThrottledItemReader<>(reader, duration);
	}
}
