package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.operation.ExpireAt;

public class ExpireAtBuilder extends AbstractMapOperationBuilder {

	private String ttl;

	public ExpireAtBuilder ttl(String field) {
		this.ttl = field;
		return this;
	}

	@Override
	protected ExpireAt<String, String, Map<String, Object>> operation(
			Function<Map<String, Object>, String> keyFunction) {
		ExpireAt<String, String, Map<String, Object>> operation = new ExpireAt<>(keyFunction);
		operation.setEpochFunction(toLong(ttl, 0));
		return operation;
	}

}
