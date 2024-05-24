package com.redis.riot.operation;

import java.util.Map;

import com.redis.spring.batch.item.redis.common.Operation;

public interface OperationCommand {

	Operation<String, String, Map<String, Object>, Object> operation();

}
