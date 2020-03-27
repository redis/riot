package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XaddMaxlen extends AbstractKeyMapCommandWriter {

	private @Setter long maxlen;
	private @Setter boolean approximateTrimming;

	@Builder
	protected XaddMaxlen(KeyBuilder keyBuilder, boolean keepKeyFields, long maxlen, boolean approximateTrimming) {
		super(keyBuilder, keepKeyFields);
		this.maxlen = maxlen;
		this.approximateTrimming = approximateTrimming;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return commands.xadd(redis, key, stringMap(item), maxlen, approximateTrimming);
	}

}