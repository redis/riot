package com.redislabs.riot.redis.writer.search;

import lombok.Setter;

public abstract class AbstractRediSearchWriter {

	@Setter
	protected String index;
}
