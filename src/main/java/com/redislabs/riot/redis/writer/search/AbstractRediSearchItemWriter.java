package com.redislabs.riot.redis.writer.search;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import lombok.Setter;

public abstract class AbstractRediSearchItemWriter extends AbstractRedisItemWriter {

	@Setter
	protected String index;
}
