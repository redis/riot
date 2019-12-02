package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

public class Hmset<R> extends AbstractKeyMapRedisWriter<R> {

	@SuppressWarnings("unchecked")
	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		return commands.hmset(redis, key, stringMap(item));
	}

}