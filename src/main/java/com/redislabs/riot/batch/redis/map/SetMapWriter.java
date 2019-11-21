package com.redislabs.riot.batch.redis.map;

import java.util.Map;

public abstract class SetMapWriter<R> extends AbstractMapWriter<R> {

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return commands.set(redis, key, value);
	}

	protected abstract String value(Map<String, Object> item);

}
