package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.operation.Lpush;

public class LpushBuilder extends AbstractCollectionMapOperationBuilder {

	@Override
	protected Lpush<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		return new Lpush<>(keyFunction, member());
	}

}
