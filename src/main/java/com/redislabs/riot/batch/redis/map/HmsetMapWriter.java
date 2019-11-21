package com.redislabs.riot.batch.redis.map;

import java.util.Map;

public class HmsetMapWriter<R> extends AbstractMapWriter<R> {

	@SuppressWarnings("unchecked")
	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		return commands.hmset(redis, key, stringMap(item));
	}

}