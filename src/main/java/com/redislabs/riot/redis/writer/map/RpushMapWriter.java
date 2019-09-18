package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class RpushMapWriter extends CollectionMapWriter {

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		return pipeline.rpush(key, member);
	}

	@Override
	protected void write(JedisCluster cluster, String key, String member, Map<String, Object> item) {
		cluster.rpush(key, member);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, String member, Map<String, Object> item) {
		return ((RedisListAsyncCommands<String, String>) commands).rpush(key, member);
	}

}