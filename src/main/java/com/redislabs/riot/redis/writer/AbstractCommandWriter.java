package com.redislabs.riot.redis.writer;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public abstract class AbstractCommandWriter<O> implements CommandWriter<O> {

	private @Setter RedisCommands commands;

	@Override
	public Object write(Object redis, O item) throws Exception {
		return write(commands, redis, item);
	}

	protected abstract Object write(RedisCommands commands, Object redis, O item) throws Exception;

}
