package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

public class SetField extends Set {

	private @Setter String field;

	@Builder
	protected SetField(KeyBuilder keyBuilder, boolean keepKeyFields, String field) {
		super(keyBuilder, keepKeyFields);
		this.field = field;
	}

	@Override
	protected String value(Map<String, Object> item) {
		return convert(item.get(field), String.class);
	}

}
