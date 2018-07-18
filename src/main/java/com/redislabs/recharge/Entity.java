package com.redislabs.recharge;

import java.util.Map;

import lombok.Data;

@Data
public class Entity {

	private String name;
	private Map<String, Object> fields;

	public Entity(String name, Map<String, Object> fields) {
		this.name = name;
		this.fields = fields;
	}

	public Object get(String field) {
		return fields.get(field);
	}
}
