package com.redislabs.riot.batch.redis.map;

import java.util.Map;

public class LpushMapWriter<R> extends CollectionMapWriter<R> {

	@Override
	protected Object write(R redis, String key, String member, Map<String, Object> item) {
		return commands.lpush(redis, key, member);
	}

}