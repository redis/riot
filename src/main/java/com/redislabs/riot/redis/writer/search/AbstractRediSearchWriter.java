package com.redislabs.riot.redis.writer.search;

import com.redislabs.riot.redis.writer.AbstractRedisWriter;

public abstract class AbstractRediSearchWriter extends AbstractRedisWriter {

	protected String index;

	public void setIndex(String index) {
		this.index = index;
	}
}
