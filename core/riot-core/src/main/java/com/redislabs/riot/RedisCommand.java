package com.redislabs.riot;

import org.springframework.batch.item.redis.RedisOperation;

public interface RedisCommand<T> {

	RedisOperation<String, String, T> operation();

}
