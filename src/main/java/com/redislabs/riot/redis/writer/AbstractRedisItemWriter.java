package com.redislabs.riot.redis.writer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public abstract class AbstractRedisItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(AbstractRedisItemWriter.class);

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
