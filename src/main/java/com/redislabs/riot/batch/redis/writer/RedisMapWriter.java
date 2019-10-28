package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public interface RedisMapWriter {

	RedisFuture<?> write(Object commands, Map<String, Object> item);

	Response<?> write(Pipeline pipeline, Map<String, Object> item);

	void write(JedisCluster cluster, Map<String, Object> item);

}
