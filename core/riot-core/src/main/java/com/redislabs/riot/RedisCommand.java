package com.redislabs.riot;

import org.springframework.batch.item.redis.OperationItemWriter;

public interface RedisCommand<T> {

	OperationItemWriter.RedisOperation<T> operation();

}
