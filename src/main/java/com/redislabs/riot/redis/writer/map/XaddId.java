package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class XaddId<R> extends Xadd<R> {

	@Setter
	private String idField;

	@Override
	protected Object doWrite(RedisCommands<R> commands, R redis, String key, Map<String, String> map) {
		return doWrite(commands, redis, key, map, map.remove(idField));
	}

	protected Object doWrite(RedisCommands<R> commands, R redis, String key, Map<String, String> map, String id) {
		return commands.xadd(redis, key, id, map);
	}

}