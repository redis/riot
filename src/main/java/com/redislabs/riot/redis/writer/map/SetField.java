package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import lombok.Setter;

public class SetField<R> extends Set<R> {

	@Setter
	private String field;

	@Override
	protected String value(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

}
