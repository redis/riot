package com.redis.riot.cli;

import java.util.Map;

import com.redis.spring.batch.operation.Operation;

public interface RedisCommand {

	Operation<String, String, Map<String, Object>, Object> operation();

}
