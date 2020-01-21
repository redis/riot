package com.redislabs.riot.redis.writer;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public abstract class AbstractRedisWriter<O> implements RedisWriter<O> {

	@Setter
	private RedisCommands commands;

	@Override
	public Object write(Object redis, O item) throws Exception {
		return write(commands, redis, item);
	}

	protected abstract Object write(RedisCommands commands, Object redis, O item) throws Exception;

}
