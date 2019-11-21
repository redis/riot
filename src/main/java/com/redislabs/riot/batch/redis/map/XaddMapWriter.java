package com.redislabs.riot.batch.redis.map;

import java.util.Map;

@SuppressWarnings("unchecked")
public class XaddMapWriter<R> extends AbstractMapWriter<R> {

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		return commands.xadd(redis, key, stringMap(item));
	}

}