package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
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