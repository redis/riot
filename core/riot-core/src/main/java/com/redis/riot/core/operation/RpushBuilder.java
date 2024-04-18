package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.operation.Rpush;

public class RpushBuilder extends AbstractCollectionMapOperationBuilder {

	@Override
	protected Rpush<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		return new Rpush<>(keyFunction, member());
	}

}
