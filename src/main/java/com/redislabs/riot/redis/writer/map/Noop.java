package com.redislabs.riot.redis.writer.map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

public class Noop<R, O> extends AbstractRedisWriter<R, O> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, O item) {
		return null;
	}

}
