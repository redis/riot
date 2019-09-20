package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class HmsetMapWriter extends RedisDataStructureMapWriter {

	@Override
	protected Response<String> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.hmset(key, stringMap(item));
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.hmset(key, stringMap(item));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, Map<String, Object> item) {
		return ((RedisHashAsyncCommands<String, String>) commands).hmset(key, stringMap(item));
	}

}