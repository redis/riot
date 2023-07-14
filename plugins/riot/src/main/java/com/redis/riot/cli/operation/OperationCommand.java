package com.redis.riot.cli.operation;

import com.redis.spring.batch.writer.WriteOperation;

public interface OperationCommand<T> {

	WriteOperation<String, String, T> operation();

}
