package com.redislabs.riot.redis.writer.map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

@SuppressWarnings("rawtypes")
public class Noop<O> extends AbstractRedisWriter<O> {

	@Override
	protected Object write(RedisCommands commands, Object redis, O item) {
		return null;
	}

}
