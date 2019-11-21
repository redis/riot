package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class SetFieldMapWriter<R> extends SetMapWriter<R> {

	@Setter
	private String field;

	@Override
	protected String value(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

}
