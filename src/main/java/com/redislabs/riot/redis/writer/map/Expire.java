package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Expire extends AbstractKeyMapCommandWriter {

	private @Setter String timeout;
	private @Setter long defaultTimeout;

	@Builder
	protected Expire(KeyBuilder keyBuilder, boolean keepKeyFields, String timeout, long defaultTimeout) {
		super(keyBuilder, keepKeyFields);
		this.timeout = timeout;
		this.defaultTimeout = defaultTimeout;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		long timeout = convert(item.getOrDefault(this.timeout, defaultTimeout), Long.class);
		return commands.expire(redis, key, timeout);
	}

}