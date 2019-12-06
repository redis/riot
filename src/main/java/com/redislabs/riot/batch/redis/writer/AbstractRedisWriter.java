package com.redislabs.riot.batch.redis.writer;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractRedisWriter<R, O> implements RedisWriter<R, O> {

	@Setter
	private RedisCommands<R> commands;

	@Override
	public Object write(R redis, O item) throws Exception {
		return write(commands, redis, item);
	}

	protected abstract Object write(RedisCommands<R> commands, R redis, O item) throws Exception;

}
