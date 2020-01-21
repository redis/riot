package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XaddId extends Xadd {

	@Setter
	private String idField;

	@Override
	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
		return doWrite(commands, redis, key, map, map.remove(idField));
	}

	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map, String id) {
		return commands.xadd(redis, key, id, map);
	}

}