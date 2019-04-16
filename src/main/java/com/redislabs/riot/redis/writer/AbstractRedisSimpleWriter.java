package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

public abstract class AbstractRedisSimpleWriter extends AbstractRedisDataStructureWriter {

	@Override
	protected RedisFuture<?> write(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		return writeSingle(key, record, commands);
	}

	protected abstract RedisFuture<?> writeSingle(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

}