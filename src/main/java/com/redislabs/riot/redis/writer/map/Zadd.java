package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Zadd extends AbstractCollectionMapWriter {

	@Setter
	private String scoreField;
	@Setter
	private double defaultScore;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		Double score = convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		return commands.zadd(redis, key, score, member);
	}

}
