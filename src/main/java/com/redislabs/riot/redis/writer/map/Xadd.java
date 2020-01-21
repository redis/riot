package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Xadd extends AbstractKeyMapRedisWriter {

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return doWrite(commands, redis, key, stringMap(item));
	}

	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map);
	}

}