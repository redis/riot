package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Xadd extends AbstractKeyMapCommandWriter {

	@Builder
	protected Xadd(KeyBuilder keyBuilder, boolean keepKeyFields) {
		super(keyBuilder, keepKeyFields);
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return doWrite(commands, redis, key, stringMap(item));
	}

	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map);
	}

}