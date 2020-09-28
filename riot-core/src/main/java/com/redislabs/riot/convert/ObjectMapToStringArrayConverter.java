package com.redislabs.riot.convert;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import lombok.Builder;

public class ObjectMapToStringArrayConverter implements Converter<Map<String, Object>, String[]> {

	private final Converter<Map<String, Object>, Object>[] extractors;

	@Builder
	public ObjectMapToStringArrayConverter(String... fields) {
		this.extractors = new FieldExtractor[fields.length];
		for (int index = 0; index < fields.length; index++) {
			extractors[index] = FieldExtractor.builder().field(fields[index]).build();
		}
	}

	@Override
	public String[] convert(Map<String, Object> source) {
		String[] array = new String[extractors.length];
		for (int index = 0; index < extractors.length; index++) {
			array[index] = extractors[index].convert(source).toString();
		}
		return array;
	}

}
