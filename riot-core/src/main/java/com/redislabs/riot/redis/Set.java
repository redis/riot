package com.redislabs.riot.redis;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Setter;

public class Set<K, V, T> extends AbstractKeyWriter<K, V, T> {

	@Setter
	private Converter<T, V> valueConverter;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisStringAsyncCommands<K, V>) commands).set(key, valueConverter.convert(item));
	}

}