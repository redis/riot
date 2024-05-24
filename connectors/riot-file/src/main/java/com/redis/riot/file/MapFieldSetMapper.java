package com.redis.riot.file;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.util.StringUtils;

public class MapFieldSetMapper implements FieldSetMapper<Map<String, Object>> {

	@Override
	public Map<String, Object> mapFieldSet(FieldSet fieldSet) {
		Map<String, Object> fields = new HashMap<>();
		String[] names = fieldSet.getNames();
		for (int index = 0; index < names.length; index++) {
			String value = fieldSet.readString(index);
			if (StringUtils.hasLength(value)) {
				fields.put(names[index], value);
			}
		}
		return fields;
	}

}
