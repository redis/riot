package com.redislabs.recharge.file;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class MapFieldSetMapper implements FieldSetMapper<Map<String, Object>> {

	@Override
	public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
		Map<String, Object> map = new HashMap<>();
		String[] names = fieldSet.getNames();
		for (int index = 0; index < names.length; index++) {
			String name = names[index];
			String value = fieldSet.readString(index);
			map.put(name, value);
		}
		return map;
	}

}
