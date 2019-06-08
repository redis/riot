package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;

public class HashWriter extends AbstractRedisDataStructureItemWriter {

	@Override
	protected void write(Pipeline pipeline, String key, Map<String, Object> item) {
		pipeline.hmset(key, stringMap(item));
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.hmset(key, stringMap(item));
	}

}