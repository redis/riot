package com.redislabs.riot.redis.writer;

import java.util.Map;

import redis.clients.jedis.Pipeline;

public interface JedisItemWriter {

	void write(Pipeline pipeline, Map<String, Object> item);

}
