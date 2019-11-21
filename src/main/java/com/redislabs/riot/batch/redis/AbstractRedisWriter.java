package com.redislabs.riot.batch.redis;

import com.redislabs.riot.batch.redis.map.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractRedisWriter<R, O> implements RedisWriter<R, O> {

	@Setter
	protected RedisCommands<R> commands;

}
