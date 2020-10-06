package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class Lpush<K, V, T> extends AbstractCollectionWriter<K, V, T> {

	@SuppressWarnings("unchecked")
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
		return ((RedisListAsyncCommands<K, V>) commands).lpush(key, memberId);
	}

}
