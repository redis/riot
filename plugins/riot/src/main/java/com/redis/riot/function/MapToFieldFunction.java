package com.redis.riot.function;

import java.util.Map;
import java.util.function.Function;

public class MapToFieldFunction implements Function<Map<String, Object>, Object> {

	private final String key;

	private Object defaultValue = null;

	public MapToFieldFunction(String key) {
		this.key = key;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public Object apply(Map<String, Object> t) {
		return t.getOrDefault(key, defaultValue);
	}

}
