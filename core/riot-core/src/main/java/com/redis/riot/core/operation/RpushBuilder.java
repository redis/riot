package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.spring.batch.writer.operation.Rpush;

public class RpushBuilder extends AbstractCollectionMapOperationBuilder {

	@Override
	protected AbstractKeyWriteOperation<String, String, Map<String, Object>> operation(
			Function<Map<String, Object>, String> keyFunction) {
		Rpush<String, String, Map<String, Object>> operation = new Rpush<>(keyFunction);
		operation.setValueFunction(member());
		return operation;
	}

}
