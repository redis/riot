package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.writer.RedisMapWriter;

import io.lettuce.core.RedisFuture;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class NoopMapWriter implements RedisMapWriter {

	@Override
	public RedisFuture<?> write(Object commands, Map<String, Object> item) {
		return null;
	}

	@Override
	public Response<?> write(Pipeline pipeline, Map<String, Object> item) {
		return null;
	}

	@Override
	public void write(JedisCluster cluster, Map<String, Object> item) {
	}

}
