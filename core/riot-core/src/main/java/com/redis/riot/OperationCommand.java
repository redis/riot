package com.redis.riot;

import com.redis.spring.batch.writer.Operation;

public interface OperationCommand<T> {

	Operation<String, String, T> operation();

}
