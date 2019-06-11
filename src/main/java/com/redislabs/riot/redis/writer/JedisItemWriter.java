package com.redislabs.riot.redis.writer;

import java.util.Map;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public interface JedisItemWriter {

	Response<?> write(Pipeline pipeline, Map<String, Object> item);

}
