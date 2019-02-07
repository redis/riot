package com.redislabs.recharge.file;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapFieldSetMapper implements FieldSetMapper<Map> {

	@Override
	public Map mapFieldSet(FieldSet fieldSet) {
		Map fields = new HashMap<>();
		String[] names = fieldSet.getNames();
		for (int index = 0; index < names.length; index++) {
			String name = names[index];
			String value = fieldSet.readString(index);
			if (value == null || value.length() == 0) {
				continue;
			}
			fields.put(name, value);
		}
		return fields;
	}

}
