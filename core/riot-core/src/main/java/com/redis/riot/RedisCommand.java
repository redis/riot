package com.redis.riot;

import org.springframework.batch.item.redis.support.RedisOperation;

public interface RedisCommand<T> {

    RedisOperation<String, String, T> operation();

}
