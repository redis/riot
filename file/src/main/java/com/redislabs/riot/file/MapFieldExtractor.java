package com.redislabs.riot.file;

import lombok.Builder;
import org.springframework.batch.item.file.transform.FieldExtractor;

import java.util.Map;

@Builder
public class MapFieldExtractor implements FieldExtractor<Map<String, String>> {

	private final String[] names;

	@Override
	public String[] extract(Map<String, String> item) {
		String[] fields = new String[names.length];
		for (int index = 0; index < names.length; index++) {
			fields[index] = item.get(names[index]);
		}
		return fields;
	}

}