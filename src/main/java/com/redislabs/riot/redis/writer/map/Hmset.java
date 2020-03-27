package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Hmset extends AbstractKeyMapCommandWriter {

	@Builder
	protected Hmset(KeyBuilder keyBuilder, boolean keepKeyFields) {
		super(keyBuilder, keepKeyFields);
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return commands.hmset(redis, key, stringMap(item));
	}

}