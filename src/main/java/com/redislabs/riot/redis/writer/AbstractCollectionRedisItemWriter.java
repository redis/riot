package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;

import lombok.Setter;

public abstract class AbstractCollectionRedisItemWriter implements RedisItemWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	protected RedisCommands commands;
	@Setter
	private String[] fields;

	protected String member(Map<String, Object> item) {
		return converter.joinFields(item, fields);
	}

}
