package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

public class Hmset<R> extends AbstractKeyRedisWriter<R> {

	@SuppressWarnings("unchecked")
	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		return commands.hmset(redis, key, stringMap(item));
	}

}