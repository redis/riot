package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Sadd extends AbstractCollectionMapWriter {

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		return commands.sadd(redis, key, member);
	}

}
