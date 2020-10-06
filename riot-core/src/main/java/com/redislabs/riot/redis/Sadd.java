package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractCollectionWriter<K, V, T> {

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
		return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, memberId);
	}

}
