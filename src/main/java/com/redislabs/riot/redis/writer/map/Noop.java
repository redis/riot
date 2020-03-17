package com.redislabs.riot.redis.writer.map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractCommandWriter;

@SuppressWarnings("rawtypes")
public class Noop<T> extends AbstractCommandWriter<T> {

	@Override
	protected Object write(RedisCommands commands, Object redis, T item) {
		return null;
	}

}
