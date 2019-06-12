package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.Setter;
import reactor.core.publisher.Mono;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@Setter
public class SetWriter extends AbstractCollectionRedisItemWriter {

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		return pipeline.sadd(key, member);
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		return commands.sadd(key, member);
	}

	@Override
	protected Mono<?> write(RedisReactiveCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		return commands.sadd(key, member);
	}

}
