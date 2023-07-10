package com.redis.riot.cli.operation;

import com.redis.spring.batch.common.Operation;

public interface OperationCommand<T> {

	Operation<String, String, T, Object> operation();

}
