package com.redislabs.riot.batch.redis.map;

import java.util.Map;

public class RpushMapWriter<R> extends CollectionMapWriter<R> {

	@Override
	protected Object write(R redis, String key, String member, Map<String, Object> item) {
		return commands.rpush(redis, key, member);
	}

}