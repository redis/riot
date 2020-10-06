package com.redislabs.riot.redis;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;

public abstract class AbstractCollectionWriter<K, V, T> extends AbstractKeyWriter<K, V, T> {

	@Setter
	private Converter<T, V> memberIdConverter;

	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return write(commands, item, key, memberIdConverter.convert(item));
	}

	protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId);
}
