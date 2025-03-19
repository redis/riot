package com.redis.riot.function;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.StreamMessage;

public class StreamToMapFunction implements Function<Collection<StreamMessage<String, String>>, Map<String, String>> {

	public static final String DEFAULT_KEY_FORMAT = "%s.%s";

	private final HashToMapFunction bodyFunction = new HashToMapFunction();

	private String keyFormat = DEFAULT_KEY_FORMAT;

	public void setKeyFormat(String keyFormat) {
		this.keyFormat = keyFormat;
	}

	@Override
	public Map<String, String> apply(Collection<StreamMessage<String, String>> source) {
		Map<String, String> result = new HashMap<>();
		for (StreamMessage<String, String> message : source) {
			Map<String, String> sortedMap = bodyFunction.apply(message.getBody());
			for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
				result.put(String.format(keyFormat, message.getId(), entry.getKey()), entry.getValue());
			}
		}
		return result;
	}

}
