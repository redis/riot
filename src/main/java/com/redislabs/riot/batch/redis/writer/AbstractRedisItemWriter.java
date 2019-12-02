package com.redislabs.riot.batch.redis.writer;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public abstract class AbstractRedisItemWriter<R, O> extends AbstractItemStreamItemWriter<O> {

	private AtomicInteger activeThreads = new AtomicInteger(0);
	@Setter
	protected RedisWriter<R, O> writer;

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

	protected void logWriteError(O item, Exception e) {
		if (log.isDebugEnabled()) {
			log.debug("Could not write record {}", item, e);
		} else {
			log.error("Could not write record: {}", e.getMessage());
		}
	}
}
