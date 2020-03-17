package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class Expire extends AbstractKeyMapCommandWriter {

	@Setter
	private String timeoutField;
	@Setter
	private Long defaultTimeout;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		long timeout = convert(item.getOrDefault(timeoutField, defaultTimeout), Long.class);
		return commands.expire(redis, key, timeout);
	}

}