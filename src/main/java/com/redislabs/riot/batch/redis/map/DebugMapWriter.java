package com.redislabs.riot.batch.redis.map;

import com.redislabs.riot.batch.redis.AbstractRedisWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DebugMapWriter<R, O> extends AbstractRedisWriter<R, O> {

	@Override
	public Object write(R redis, O item) {
		log.info("{}", item);
		return null;
	}

}
