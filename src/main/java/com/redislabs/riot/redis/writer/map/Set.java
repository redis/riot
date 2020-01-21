package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class Set extends AbstractKeyMapRedisWriter {

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return commands.set(redis, key, value);
	}

	protected abstract String value(Map<String, Object> item);

}
