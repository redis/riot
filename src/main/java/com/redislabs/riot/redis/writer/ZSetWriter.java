package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import redis.clients.jedis.Pipeline;

public class ZSetWriter extends AbstractCollectionRedisItemWriter {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore;

	private double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	protected void write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		pipeline.zadd(key, score(item), member);
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		return commands.zadd(key, score(item), member);
	}

}
