package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Setter;

@Setter
public class ListWriter extends AbstractCollectionRedisItemWriter {

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		return commands.lpush(redis, key(item), member(item));
	}

}