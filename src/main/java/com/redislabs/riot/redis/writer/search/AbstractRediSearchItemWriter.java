package com.redislabs.riot.redis.writer.search;

import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.RedisCommands;
import com.redislabs.riot.redis.writer.RedisItemWriter;

import lombok.Setter;

public abstract class AbstractRediSearchItemWriter implements RedisItemWriter {
	@Setter
	protected RedisConverter converter;
	@Setter
	protected RedisCommands commands;
	@Setter
	protected String index;
}
