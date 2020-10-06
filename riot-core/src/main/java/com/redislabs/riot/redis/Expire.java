package com.redislabs.riot.redis;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;

public class Expire<K, V, T> extends AbstractKeyWriter<K, V, T> {

	@Setter
	private Converter<T, Long> timeoutConverter;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Long millis = timeoutConverter.convert(item);
		if (millis == null) {
			return null;
		}
		return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, millis);
	}

}