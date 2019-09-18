package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class SaddMapWriter extends CollectionMapWriter {

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		return pipeline.sadd(key, member);
	}

	@Override
	protected void write(JedisCluster cluster, String key, String member, Map<String, Object> item) {
		cluster.sadd(key, member);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, String member, Map<String, Object> item) {
		return ((RedisSetAsyncCommands<String, String>) commands).sadd(key, member);
	}

}
