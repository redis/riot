package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.operation.Sadd;

public class SaddBuilder extends AbstractCollectionMapOperationBuilder {

	@Override
	protected Sadd<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		return new Sadd<>(keyFunction, member());
	}

}
