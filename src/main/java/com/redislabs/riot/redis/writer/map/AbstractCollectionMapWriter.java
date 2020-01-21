package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public abstract class AbstractCollectionMapWriter extends AbstractKeyMapRedisWriter {

	@Setter
	private KeyBuilder memberIdBuilder;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		String member = memberIdBuilder.key(item);
		return write(commands, redis, key, member, item);
	}

	protected abstract Object write(RedisCommands commands, Object redis, String key, String member,
			Map<String, Object> item);

}
