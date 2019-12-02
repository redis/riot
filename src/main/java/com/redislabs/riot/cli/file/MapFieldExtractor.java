package com.redislabs.riot.cli.file;

import java.util.Map;

import org.springframework.batch.item.file.transform.FieldExtractor;

public class MapFieldExtractor implements FieldExtractor<Map<String, Object>> {

	private String[] names;

	public MapFieldExtractor(String[] names) {
		this.names = names;
	}

	@Override
	public Object[] extract(Map<String, Object> item) {
		Object[] fields = new Object[names.length];
		for (int index = 0; index < names.length; index++) {
			fields[index] = item.get(names[index]);
		}
		return fields;
	}

}