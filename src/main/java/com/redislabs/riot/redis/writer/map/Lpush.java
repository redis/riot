package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Lpush extends AbstractCollectionMapCommandWriter {

	@Builder
	protected Lpush(KeyBuilder keyBuilder, boolean keepKeyFields, KeyBuilder memberIdBuilder) {
		super(keyBuilder, keepKeyFields, memberIdBuilder);
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		return commands.lpush(redis, key, member);
	}

}