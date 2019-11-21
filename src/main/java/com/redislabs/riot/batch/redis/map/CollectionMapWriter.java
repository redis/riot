package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import lombok.Setter;

public abstract class CollectionMapWriter<R> extends AbstractMapWriter<R> {

	@Setter
	private String[] fields = new String[0];

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		String member = join(item, fields);
		return write(redis, key, member, item);
	}

	protected abstract Object write(R redis, String key, String member, Map<String, Object> item);

}
