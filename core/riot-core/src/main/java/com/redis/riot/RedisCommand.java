package com.redis.riot;

import com.redis.spring.batch.support.RedisOperation;

public interface RedisCommand<T> {

	RedisOperation<String, String, T> operation();

}
