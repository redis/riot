package com.redislabs.riot.convert;

import io.lettuce.core.StreamMessage;
import lombok.Builder;
import lombok.Setter;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamToStringMapConverter implements Converter<List<StreamMessage<String, String>>, Map<String, String>> {

	public static final String DEFAULT_KEY_FORMAT = "%s.%s";

	@Setter
	private String keyFormat = DEFAULT_KEY_FORMAT;

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
