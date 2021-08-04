package com.redislabs.riot;

import org.springframework.batch.item.redis.OperationItemWriter;

public interface RedisCommand<T> {

    OperationItemWriter.RedisOperation<String, String, T> operation();

}
