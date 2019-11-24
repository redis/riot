package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class SetField<R> extends Set<R> {

	@Setter
	private String field;

	@Override
	protected String value(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

}
