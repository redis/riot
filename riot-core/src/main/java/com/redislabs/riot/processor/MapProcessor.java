package com.redislabs.riot.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.convert.RegexNamedGroupsExtractor;

import lombok.Builder;

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

	@Builder
	private static MapProcessor build(Map<String, String> regexes) {
		Map<String, Converter<String, Map<String, String>>> extractors = new LinkedHashMap<>();
		for (String field : regexes.keySet()) {
			extractors.put(field, RegexNamedGroupsExtractor.builder().regex(regexes.get(field)).build());
		}
		return new MapProcessor(extractors);
	}

}
