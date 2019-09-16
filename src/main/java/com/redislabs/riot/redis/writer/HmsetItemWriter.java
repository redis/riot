package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class HmsetItemWriter extends RedisItemWriter<RedisHashAsyncCommands<String, String>> {

	@Override
	protected Response<String> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.hmset(key, stringMap(item));
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.hmset(key, stringMap(item));
	}

	@Override
	protected RedisFuture<?> write(RedisHashAsyncCommands<String, String> commands, String key,
			Map<String, Object> item) {
		return commands.hmset(key, stringMap(item));
	}

}