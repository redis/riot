package com.redis.riot.core.processor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class CollectionToMapFunction implements Function<Collection<String>, Map<String, String>> {

	public static final String DEFAULT_KEY_FORMAT = "%s";

	private String keyFormat = DEFAULT_KEY_FORMAT;

	public void setKeyFormat(String keyFormat) {
		this.keyFormat = keyFormat;
	}

	@Override
	public Map<String, String> apply(Collection<String> source) {
		Map<String, String> result = new HashMap<>();
		int index = 0;
		for (String element : source) {
			result.put(String.format(keyFormat, index), element);
			index++;
		}
		return result;
	}

}
