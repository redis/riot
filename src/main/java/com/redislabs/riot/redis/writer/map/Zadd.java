package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class Zadd extends AbstractCollectionMapCommandWriter {

	@Setter
	private String score;
	@Setter
	private double defaultScore;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		return commands.zadd(redis, key, convert(item.getOrDefault(score, defaultScore), Double.class), member);
	}

}
