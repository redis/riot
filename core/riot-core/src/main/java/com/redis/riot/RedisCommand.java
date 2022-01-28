package com.redis.riot;

import com.redis.spring.batch.writer.RedisOperation;

public interface RedisCommand<T> {

	RedisOperation<String, String, T> operation();

}
