package com.redislabs.riot.redis.writer;

import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	public AbstractRedisWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	public String getName() {
		return getExecutionContextKey("name");
	}

}
