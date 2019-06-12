package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class AbstractRedisDataStructureItemWriter extends AbstractRedisItemWriter
		implements JedisItemWriter, LettuceItemWriter {

	@Override
	public Response<?> write(Pipeline pipeline, Map<String, Object> item) {
		return write(pipeline, key(item), item);
	}

	protected abstract Response<?> write(Pipeline pipeline, String key, Map<String, Object> item);

	@Override
	public RedisFuture<?> write(RedisAsyncCommands<String, String> commands, Map<String, Object> item) {
		return write(commands, key(item), item);
	}

	protected abstract RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key,
			Map<String, Object> item);

	@Override
	public Mono<?> write(RedisReactiveCommands<String, String> commands, Map<String, Object> item) {
		return write(commands, key(item), item);
	}

	protected abstract Mono<?> write(RedisReactiveCommands<String, String> commands, String key,
			Map<String, Object> item);

}
