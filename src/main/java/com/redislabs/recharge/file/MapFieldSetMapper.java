package com.redislabs.recharge.file;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

import com.redislabs.recharge.Entity;

public class MapFieldSetMapper implements FieldSetMapper<Entity> {

	private String entityName;

	public MapFieldSetMapper(String entityName) {
		this.entityName = entityName;
	}

	@Override
	public Entity mapFieldSet(FieldSet fieldSet) {
		Map<String, Object> fields = new HashMap<>();
		String[] names = fieldSet.getNames();
		for (int index = 0; index < names.length; index++) {
			String name = names[index];
			String value = fieldSet.readString(index);
			fields.put(name, value);
		}
		return new Entity(entityName, fields);
	}

}
