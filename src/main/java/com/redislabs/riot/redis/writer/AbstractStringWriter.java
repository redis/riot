package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class AbstractStringWriter extends AbstractRedisDataStructureItemWriter {

	@Override
	protected Response<String> write(Pipeline pipeline, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return pipeline.set(key, value);
	}

	protected abstract String value(Map<String, Object> item);

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return commands.set(key, value);
	}

}
