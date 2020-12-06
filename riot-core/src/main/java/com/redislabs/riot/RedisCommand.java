package com.redislabs.riot;

import org.springframework.batch.item.ItemWriter;

import io.lettuce.core.AbstractRedisClient;

public interface RedisCommand<T> {

	ItemWriter<T> writer(AbstractRedisClient client, RedisOptions redisOptions) throws Exception;
}
