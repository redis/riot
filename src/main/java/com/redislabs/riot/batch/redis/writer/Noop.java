package com.redislabs.riot.batch.redis.writer;

import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.RedisCommands;

public class Noop<R, O> extends AbstractRedisWriter<R, O> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, O item) {
		return null;
	}

}
