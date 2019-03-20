package com.redislabs.recharge.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public abstract class SingleRedisWriter<T extends RedisCommandConfiguration> extends RedisCommandWriter<T> {

	protected SingleRedisWriter(T config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String id, Map record, RediSearchAsyncCommands<String, String> commands) {
		return writeSingle(getKey(id), record, commands);
	}

	protected abstract RedisFuture<?> writeSingle(String key, Map record,
			RediSearchAsyncCommands<String, String> commands);

}