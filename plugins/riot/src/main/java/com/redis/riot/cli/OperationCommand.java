package com.redis.riot.cli;

import com.redis.spring.batch.writer.Operation;

public interface OperationCommand<T> {

	Operation<String, String, T> operation();

}
