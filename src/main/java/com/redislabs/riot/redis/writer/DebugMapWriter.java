package com.redislabs.riot.redis.writer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisFuture;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class DebugMapWriter implements RedisMapWriter {

	private final Logger log = LoggerFactory.getLogger(DebugMapWriter.class);

	@Override
	public RedisFuture<?> write(Object commands, Map<String, Object> item) {
		debug(item);
		return null;
	}

	private void debug(Map<String, Object> item) {
		log.info("{}", item);
	}

	@Override
	public Response<?> write(Pipeline pipeline, Map<String, Object> item) {
		debug(item);
		return null;
	}

	@Override
	public void write(JedisCluster cluster, Map<String, Object> item) {
		debug(item);
	}

}
