package com.redis.riot.core.processor;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

public class MapProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private final Map<String, Converter<String, Map<String, String>>> extractors;

	public MapProcessor(Map<String, Converter<String, Map<String, String>>> extractors) {
		this.extractors = extractors;
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) {
		for (Entry<String, Converter<String, Map<String, String>>> extractor : extractors.entrySet()) {
			Object value = item.get(extractor.getKey());
			if (value == null) {
				continue;
			}
			item.putAll(extractor.getValue().convert(value.toString()));
		}
		return item;
	}

}
