package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Sadd extends AbstractCollectionMapCommandWriter {

	@Builder
	protected Sadd(KeyBuilder keyBuilder, boolean keepKeyFields, KeyBuilder memberIdBuilder) {
		super(keyBuilder, keepKeyFields, memberIdBuilder);
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		return commands.sadd(redis, key, member);
	}

}
