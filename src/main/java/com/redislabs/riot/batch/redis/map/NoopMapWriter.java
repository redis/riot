package com.redislabs.riot.batch.redis.map;

import com.redislabs.riot.batch.redis.AbstractRedisWriter;

public class NoopMapWriter<R, O> extends AbstractRedisWriter<R, O> {

	@Override
	public Object write(R redis, O item) {
		return null;
	}

}
