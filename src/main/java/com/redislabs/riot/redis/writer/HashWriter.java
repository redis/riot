package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;

import lombok.Setter;

public class HashWriter implements RedisItemWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	protected RedisCommands commands;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		String key = converter.key(item);
		return commands.hmset(redis, key, item);
	}

}