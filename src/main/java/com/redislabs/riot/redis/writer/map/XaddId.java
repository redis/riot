package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XaddId extends AbstractKeyMapCommandWriter {

	private @Setter String id;

	@Builder
	protected XaddId(KeyBuilder keyBuilder, boolean keepKeyFields, String id) {
		super(keyBuilder, keepKeyFields);
		this.id = id;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return commands.xadd(redis, key, convert(item.get(id), String.class), stringMap(item));
	}

}