package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public abstract class AbstractKeyMapRedisWriter extends AbstractMapRedisWriter {

	@Setter
	private KeyBuilder keyBuilder;

	@SuppressWarnings("unchecked")
	protected Map stringMap(Map map) {
		map.forEach((k, v) -> map.put(k, convert(v, String.class)));
		return map;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, Map<String, Object> item) {
		String key = keyBuilder.key(item);
		return write(commands, redis, key, item);
	}

	protected abstract Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item);

}
