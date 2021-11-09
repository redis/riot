package com.redis.riot.convert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.StreamMessage;

public class StreamToStringMapConverter implements Converter<List<StreamMessage<String, String>>, Map<String, String>> {

	public static final String DEFAULT_KEY_FORMAT = "%s.%s";

	private String keyFormat = DEFAULT_KEY_FORMAT;

	public void setKeyFormat(String keyFormat) {
		this.keyFormat = keyFormat;
	}

	@Override
	public Map<String, String> convert(List<StreamMessage<String, String>> source) {
		Map<String, String> result = new HashMap<>();
		for (StreamMessage<String, String> message : source) {
			for (Map.Entry<String, String> entry : message.getBody().entrySet()) {
				result.put(String.format(keyFormat, message.getId(), entry.getKey()), entry.getValue());
			}
		}
		return result;
	}
}
