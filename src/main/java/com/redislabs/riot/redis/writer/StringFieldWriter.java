package com.redislabs.riot.redis.writer;

import java.util.Map;

public class StringFieldWriter extends AbstractStringWriter {

	private String field;

	public StringFieldWriter(String field) {
		this.field = field;
	}

	@Override
	protected String value(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

}
