package com.redislabs.recharge.redisearch;

import com.redislabs.recharge.redis.writer.AbstractRedisWriter;

public abstract class AbstractRediSearchWriter extends AbstractRedisWriter {

	protected String index;

	public void setIndex(String index) {
		this.index = index;
	}
}
