package com.redislabs.riot.batch.redis.writer;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public abstract class AbstractRedisItemWriter<R, O> extends AbstractItemStreamItemWriter<O> {

	@Setter
	protected RedisWriter<R, O> writer;

	public AbstractRedisItemWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
	}

	@Override
	public synchronized void close() {
		super.close();
	}

	protected void logWriteError(O item, Exception e) {
		if (log.isDebugEnabled()) {
			log.debug("Could not write record {}", item, e);
		} else {
			log.error("Could not write record: {}", e.getMessage());
		}
	}
}
