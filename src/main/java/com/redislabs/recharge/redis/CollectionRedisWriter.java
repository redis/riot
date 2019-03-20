package com.redislabs.recharge.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public abstract class CollectionRedisWriter<T extends CollectionRedisConfiguration> extends RedisCommandWriter<T> {

	protected CollectionRedisWriter(T config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String id, Map record, RediSearchAsyncCommands<String, String> commands) {
		String member = getValues(record, config.getFields());
		return write(getKey(id), member, record, commands);
	}

	protected abstract RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands);

}