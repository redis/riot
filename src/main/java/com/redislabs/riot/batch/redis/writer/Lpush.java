package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

public class Lpush<R> extends AbstractCollectionMapWriter<R> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, String member, Map<String, Object> item) {
		return commands.lpush(redis, key, member);
	}

}