package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@Slf4j
public class DebugMapWriter implements RedisMapWriter {

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
