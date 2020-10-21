package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
		return null;
	}

}
