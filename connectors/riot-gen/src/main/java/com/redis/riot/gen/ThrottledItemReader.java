package com.redis.riot.gen;

import org.springframework.batch.item.ItemReader;

public class ThrottledItemReader<T> implements ItemReader<T> {

    private final ItemReader<T> delegate;
    private final long sleep;

    public ThrottledItemReader(ItemReader<T> delegate, long sleepDurationInMillis) {
        this.delegate = delegate;
        this.sleep = sleepDurationInMillis;
    }

    @Override
    public T read() throws Exception {
        Thread.sleep(sleep);
        return delegate.read();
    }
}
