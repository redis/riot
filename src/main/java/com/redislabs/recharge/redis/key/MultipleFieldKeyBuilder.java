package com.redislabs.recharge.redis.key;

import java.util.Arrays;
import java.util.Map;

public class MultipleFieldKeyBuilder extends AbstractKeyBuilder {

	private String[] fields;

	public MultipleFieldKeyBuilder(String[] fields) {
		this.fields = fields;
	}

	@Override
	public String getId(Map<String, Object> entity) {
		String[] values = new String[fields.length];
		Arrays.setAll(values, index -> getString(entity, fields[index]));
		return join(values);
	}

	public static String join(String... values) {
		return String.join(KEY_SEPARATOR, values);
	}

}