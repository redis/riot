package com.redislabs.riot.redis.writer;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public abstract class AbstractRedisWriter<R, O> implements RedisWriter<R, O> {

	@Setter
	private RedisCommands<R> commands;

	@Override
	public Object write(R redis, O item) throws Exception {
		return write(commands, redis, item);
	}

	protected abstract Object write(RedisCommands<R> commands, R redis, O item) throws Exception;

}
