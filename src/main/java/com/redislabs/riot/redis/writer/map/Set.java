package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

public abstract class Set<R> extends AbstractKeyMapRedisWriter<R> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return commands.set(redis, key, value);
	}

	protected abstract String value(Map<String, Object> item);

}
