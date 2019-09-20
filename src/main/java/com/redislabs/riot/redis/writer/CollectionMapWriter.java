package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class CollectionMapWriter extends RedisDataStructureMapWriter {

	private String[] fields = new String[0];

	public void setFields(String[] fields) {
		this.fields = fields;
	}

	private String member(Map<String, Object> item) {
		return join(item, fields);
	}

	@Override
	protected Response<?> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return write(pipeline, key, member(item), item);
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		write(cluster, key, member(item), item);
	}

	protected abstract void write(JedisCluster cluster, String key, String member, Map<String, Object> item);

	protected abstract Response<?> write(Pipeline pipeline, String key, String member, Map<String, Object> item);

	@Override
	protected RedisFuture<?> write(Object commands, String key, Map<String, Object> item) {
		return write(commands, key, member(item), item);
	}

	protected abstract RedisFuture<?> write(Object commands, String key, String member, Map<String, Object> item);

}
