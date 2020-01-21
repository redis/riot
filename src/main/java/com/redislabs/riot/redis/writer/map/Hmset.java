package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Hmset extends AbstractKeyMapRedisWriter {

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return commands.hmset(redis, key, stringMap(item));
	}

}