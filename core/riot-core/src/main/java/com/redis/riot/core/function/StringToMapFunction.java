package com.redis.riot.core.function;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class StringToMapFunction implements Function<String, Map<String, String>> {

	public static final String DEFAULT_KEY = "value";
	public static final UnaryOperator<String> DEFAULT_KEY_EXTRACTOR = s -> DEFAULT_KEY;

	private UnaryOperator<String> keyExtractor = DEFAULT_KEY_EXTRACTOR;

	public void setKeyExtractor(UnaryOperator<String> keyExtractor) {
		this.keyExtractor = keyExtractor;
	}

	@Override
	public Map<String, String> apply(String t) {
		Map<String, String> result = new HashMap<>();
		result.put(keyExtractor.apply(t), t);
		return result;
	}

}
