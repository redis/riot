package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class SetItemWriter extends RedisItemWriter<RedisStringAsyncCommands<String, String>> {

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
		if (value==null) {
			return;
		}
		cluster.set(key, value);
	}

	protected abstract String value(Map<String, Object> item);

	@Override
	protected RedisFuture<?> write(RedisStringAsyncCommands<String, String> commands, String key,
			Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return commands.set(key, value);
	}

}
