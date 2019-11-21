package com.redislabs.riot.batch.redis;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisItemWriter<O> extends AbstractItemStreamItemWriter<O> {

	private AtomicInteger activeThreads = new AtomicInteger(0);

	public AbstractRedisItemWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		int threads = activeThreads.incrementAndGet();
		log.debug("Opened Redis writer, {} active threads", threads);
		super.open(executionContext);
	}

	@Override
	public synchronized void close() {
		super.close();
		int threads = activeThreads.decrementAndGet();
		log.debug("Closed Redis writer, {} active threads", threads);
	}

	protected boolean hasActiveThreads() {
		return activeThreads.get() > 0;
	}
}
