package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class Zadd<R> extends AbstractCollectionMapWriter<R> {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, String member, Map<String, Object> item) {
		Double score = convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		return commands.zadd(redis, key, score, member);
	}

}
