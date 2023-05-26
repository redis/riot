package com.redis.riot.core.convert;

import java.util.Map;
import java.util.function.Function;

public class MapToStringArrayConverter implements Function<Map<String, Object>, String[]> {

	private final Function<Map<String, Object>, String>[] fieldConverters;

	public MapToStringArrayConverter(Function<Map<String, Object>, String>[] fieldConverters) {
		this.fieldConverters = fieldConverters;
	}

	@Override
	public String[] apply(Map<String, Object> source) {
		String[] array = new String[fieldConverters.length];
		for (int index = 0; index < fieldConverters.length; index++) {
			array[index] = fieldConverters[index].apply(source);
		}
		return array;
	}

}
