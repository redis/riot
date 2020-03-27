package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XaddIdMaxlen extends AbstractKeyMapCommandWriter {

	private @Setter String id;
	private @Setter long maxlen;
	private @Setter boolean approximateTrimming;

	@Builder
	protected XaddIdMaxlen(KeyBuilder keyBuilder, boolean keepKeyFields, String id, long maxlen,
			boolean approximateTrimming) {
		super(keyBuilder, keepKeyFields);
		this.id = id;
		this.maxlen = maxlen;
		this.approximateTrimming = approximateTrimming;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return commands.xadd(redis, key, convert(item.get(id), String.class), stringMap(item), maxlen,
				approximateTrimming);
	}

}