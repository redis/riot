package com.redislabs.riot.redis.writer;

import java.util.Map;

public class HashWriter extends AbstractRedisItemWriter {

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		return commands.hmset(redis, key(item), item);
	}

}