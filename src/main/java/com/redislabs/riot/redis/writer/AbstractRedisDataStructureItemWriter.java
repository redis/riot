package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;

public abstract class AbstractRedisDataStructureItemWriter extends AbstractRedisItemWriter
		implements JedisItemWriter, LettuceItemWriter {

	@Override
	public void write(Pipeline pipeline, Map<String, Object> item) {
		write(pipeline, key(item), item);
	}

	protected abstract void write(Pipeline pipeline, String key, Map<String, Object> item);

	@Override
	public RedisFuture<?> write(RedisAsyncCommands<String, String> commands, Map<String, Object> item) {
		return write(commands, key(item), item);
	}

	protected abstract RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key,
			Map<String, Object> item);

}
