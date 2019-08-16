package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class SaddWriter extends CollectionItemWriter {

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		return pipeline.sadd(key, member);
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		return commands.sadd(key, member);
	}

}
