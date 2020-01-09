package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

@SuppressWarnings("unchecked")
public class Xadd<R> extends AbstractKeyMapRedisWriter<R> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		return doWrite(commands, redis, key, stringMap(item));
	}

	protected Object doWrite(RedisCommands<R> commands, R redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map);
	}

}