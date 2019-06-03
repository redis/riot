package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import org.springframework.core.convert.ConversionService;

import lombok.Setter;

@Setter
public class SuggestWriter extends AbstractRediSearchItemWriter {

	private String field;
	private String scoreField;
	private double defaultScore = 1d;
	private boolean increment;
	private String payloadField;
	private ConversionService converter;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		String string = converter.convert(item.get(field), String.class);
		double score = converter.convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		if (string == null) {
			return null;
		}
		String payload = payload(item);
		return commands.sugadd(redis, index, string, score, increment, payload);
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return converter.convert(item.remove(payloadField), String.class);
	}

}
