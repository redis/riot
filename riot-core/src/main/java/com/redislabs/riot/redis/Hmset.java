package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import lombok.Setter;

public class Hmset<K, V, T> extends AbstractKeyWriter<K, V, T> {

	@Setter
	private Converter<T, Map<K, V>> mapConverter;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisHashAsyncCommands<K, V>) commands).hmset(key, mapConverter.convert(item));
	}

}
