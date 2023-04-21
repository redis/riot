package com.redis.riot.core.convert;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

public class CollectionToStringMapConverter implements Converter<Collection<String>, Map<String, String>> {

	public static final String DEFAULT_KEY_FORMAT = "[%s]";

	private String keyFormat = DEFAULT_KEY_FORMAT;

	public void setKeyFormat(String keyFormat) {
		this.keyFormat = keyFormat;
	}

	@Override
	public Map<String, String> convert(Collection<String> source) {
		Map<String, String> result = new HashMap<>();
		int index = 0;
		for (String element : source) {
			result.put(String.format(keyFormat, index), element);
			index++;
		}
		return result;
	}
}
