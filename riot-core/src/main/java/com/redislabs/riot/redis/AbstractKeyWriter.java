package com.redislabs.riot.redis;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;

public abstract class AbstractKeyWriter<K, V, T> extends AbstractRedisWriter<K, V, T> {

	@Setter
	private Converter<T, K> keyConverter;

	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
		return write(commands, item, keyConverter.convert(item));
	}

	protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key);
}
