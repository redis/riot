package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

public class ZSetWriter extends AbstractRedisCollectionWriter {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore;

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		double score = score(record);
		return commands.zadd(key, score, member);
	}

	private double score(Map<String, Object> record) {
		return converter.convert(record.getOrDefault(scoreField, defaultScore), Double.class);
	}

}
