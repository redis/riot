package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.operation.Lpush;

public class LpushBuilder extends AbstractCollectionMapOperationBuilder {

	@Override
	protected Lpush<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		Lpush<String, String, Map<String, Object>> operation = new Lpush<>(keyFunction);
		operation.setValueFunction(member());
		return operation;
	}

}
