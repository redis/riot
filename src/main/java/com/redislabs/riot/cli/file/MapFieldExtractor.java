package com.redislabs.riot.cli.file;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.file.transform.FieldExtractor;

public class MapFieldExtractor implements FieldExtractor<Map<String, Object>> {

	private List<String> names;

	public MapFieldExtractor(List<String> names) {
		this.names = names;
	}

	@Override
	public Object[] extract(Map<String, Object> item) {
		Object[] fields = new Object[names.size()];
		for (int index = 0; index < names.size(); index++) {
			fields[index] = item.get(names.get(index));
		}
		return fields;
	}

}