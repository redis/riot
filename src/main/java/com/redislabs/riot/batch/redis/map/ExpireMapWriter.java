package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class ExpireMapWriter<R> extends AbstractMapWriter<R> {

	@Setter
	private String timeoutField;
	@Setter
	private Long defaultTimeout;

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		long timeout = convert(item.getOrDefault(timeoutField, defaultTimeout), Long.class);
		return commands.expire(redis, key, timeout);
	}

}