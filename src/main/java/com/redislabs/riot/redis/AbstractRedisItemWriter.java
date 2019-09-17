package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public abstract class AbstractRedisItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private int activeThreads = 0;

	public AbstractRedisItemWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		activeThreads++;
		super.open(executionContext);
	}

	@Override
	public synchronized void close() {
		super.close();
		activeThreads--;
	}

	protected boolean hasActiveThreads() {
		return activeThreads > 0;
	}
}
