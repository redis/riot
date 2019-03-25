package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

public abstract class AbstractSingleRedisWriter extends AbstractRedisCommandWriter {

	@Override
	protected RedisFuture<?> write(String id, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		return writeSingle(getKey(id), record, commands);
	}

	protected abstract RedisFuture<?> writeSingle(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

}