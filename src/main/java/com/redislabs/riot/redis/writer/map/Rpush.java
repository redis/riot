package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

public class Rpush<R> extends AbstractCollectionMapWriter<R> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, String member, Map<String, Object> item) {
		return commands.rpush(redis, key, member);
	}

}