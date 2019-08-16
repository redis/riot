package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class ZaddItemWriter extends CollectionItemWriter {

	private String scoreField;
	private double defaultScore;

	public void setScoreField(String scoreField) {
		this.scoreField = scoreField;
	}

	public void setDefaultScore(double defaultScore) {
		this.defaultScore = defaultScore;
	}

	private double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		return pipeline.zadd(key, score(item), member);
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		return commands.zadd(key, score(item), member);
	}

}
