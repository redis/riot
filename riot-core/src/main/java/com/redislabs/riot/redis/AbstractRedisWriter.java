package com.redislabs.riot.redis;

import java.util.List;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractRedisWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	@Override
	protected void write(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		RedisFuture<?> future = write(commands, item);
		if (future == null) {
			return;
		}
		futures.add(future);
	}

	protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item);

}
