package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Setter;

@Setter
public class SetWriter extends AbstractCollectionRedisItemWriter {

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		return commands.sadd(redis, key(item), member(item));
	}

}
