package com.redislabs.riot;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;

public interface RedisCommand<T> {

	ItemWriter<T> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception;
}
