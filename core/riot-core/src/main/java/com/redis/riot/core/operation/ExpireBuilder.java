package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.Expire;

public class ExpireBuilder extends AbstractMapOperationBuilder {

	private String ttlField;

	public ExpireBuilder ttlField(String ttl) {
		this.ttlField = ttl;
		return this;
	}

	@Override
	protected Expire<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		Expire<String, String, Map<String, Object>> operation = new Expire<>(keyFunction);
		operation.ttl(toLong(ttlField));
		return operation;
	}

}
