package com.redislabs.recharge.flatfile;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class MapFieldSetMapper implements FieldSetMapper<Map<String, String>> {

	@Override
	public Map<String, String> mapFieldSet(FieldSet fieldSet) throws BindException {
		Map<String, String> map = new LinkedHashMap<>();
		String[] names = fieldSet.getNames();
		String[] values = fieldSet.getValues();
		for (int index = 0; index < values.length; index++) {
			map.put(names[index], values[index]);
		}
		return map;
	}

}
