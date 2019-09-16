package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class RedisItemWriter<C> extends AbstractRedisItemWriter<C> {

	@Override
	public Response<?> write(Pipeline pipeline, Map<String, Object> item) {
		return write(pipeline, key(item), item);
	}

	@Override
	public void write(JedisCluster cluster, Map<String, Object> item) {
		write(cluster, key(item), item);
	}

	protected abstract Response<?> write(Pipeline pipeline, String key, Map<String, Object> item);

	protected abstract void write(JedisCluster cluster, String key, Map<String, Object> item);

	@Override
	public RedisFuture<?> write(C commands, Map<String, Object> item) {
		return write(commands, key(item), item);
	}

	protected abstract RedisFuture<?> write(C commands, String key, Map<String, Object> item);

	@Override
	public String toString() {
		return String.format("Redis %s", getConverter());
	}

}
