package com.redislabs.riot.batch.redis.writer.map;

import com.redislabs.riot.batch.redis.RedisCommands;
import com.redislabs.riot.batch.redis.writer.AbstractRedisWriter;

public class Noop<R, O> extends AbstractRedisWriter<R, O> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, O item) {
		return null;
	}

}
