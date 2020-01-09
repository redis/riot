package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class Expire<R> extends AbstractKeyMapRedisWriter<R> {

	@Setter
	private String timeoutField;
	@Setter
	private Long defaultTimeout;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		long timeout = convert(item.getOrDefault(timeoutField, defaultTimeout), Long.class);
		return commands.expire(redis, key, timeout);
	}

}