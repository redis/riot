package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class HashWriter extends AbstractRedisDataStructureItemWriter {

	@Override
	protected Response<String> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.hmset(key, stringMap(item));
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.hmset(key, stringMap(item));
	}

	@Override
	protected Mono<String> write(RedisReactiveCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.hmset(key, stringMap(item));
	}

}