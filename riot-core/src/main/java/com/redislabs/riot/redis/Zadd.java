package com.redislabs.riot.redis;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;

public class Zadd<K, V, T> extends AbstractCollectionWriter<K, V, T> {

	@Setter
	private Converter<T, Double> scoreConverter;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
		Double score = scoreConverter.convert(item);
		if (score == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, score, memberId);
	}

}
