package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class SetMapWriter extends AbstractRedisFlatMapWriter {

	@Override
	protected Response<String> write(Pipeline pipeline, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return pipeline.set(key, value);
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return;
		}
		cluster.set(key, value);
	}

	protected abstract String value(Map<String, Object> item);

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return ((RedisStringAsyncCommands<String, String>) commands).set(key, value);
	}

}
