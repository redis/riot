package com.redislabs.riot.processor;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

public class MapProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private final Map<String, Converter<String, Map<String, String>>> extractors;

	public MapProcessor(Map<String, Converter<String, Map<String, String>>> extractors) {
		this.extractors = extractors;
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) {
		for (String field : extractors.keySet()) {
			Object value = item.get(field);
			if (value == null) {
				continue;
			}
			item.putAll(extractors.get(field).convert(value.toString()));
		}
		return item;
	}

}
