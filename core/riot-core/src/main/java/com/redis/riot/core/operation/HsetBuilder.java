package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.operation.Hset;

public class HsetBuilder extends AbstractFilterMapOperationBuilder {

	@Override
	protected Hset<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		return new Hset<>(keyFunction, map());
	}

}
