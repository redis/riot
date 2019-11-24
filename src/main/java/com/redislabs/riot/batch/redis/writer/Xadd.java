package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

@SuppressWarnings("unchecked")
public class Xadd<R> extends AbstractKeyRedisWriter<R> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		return doWrite(commands, redis, key, stringMap(item));
	}

	protected Object doWrite(RedisCommands<R> commands, R redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map);
	}

}