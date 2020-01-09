package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

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
