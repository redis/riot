package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class XaddIdMapWriter<R> extends AbstractMapWriter<R> {

	@Setter
	private String idField;

	@SuppressWarnings("unchecked")
	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		String id = convert(item.remove(idField), String.class);
		return commands.xadd(redis, key, id, stringMap(item));
	}

}