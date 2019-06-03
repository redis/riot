package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;

import lombok.Setter;

public abstract class AbstractRedisItemWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	protected RedisCommands commands;

	public abstract Object write(Object redis, Map<String, Object> item);

}