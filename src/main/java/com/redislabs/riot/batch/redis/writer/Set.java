package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

public abstract class Set<R> extends AbstractKeyRedisWriter<R> {

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
