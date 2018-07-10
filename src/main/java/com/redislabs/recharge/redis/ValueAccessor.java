package com.redislabs.recharge.redis;

import java.util.Map;

public class ValueAccessor implements IValueAccessor {

	private String[] fields;

	public ValueAccessor(String[] fields) {
		this.fields = fields;
	}

	@Override
	public Object[] getValueArray(Map<String, Object> map) {
		Object[] values = new Object[fields.length];
		for (int index = 0; index < values.length; index++) {
			values[index] = map.get(fields[index]);
		}
		return values;
	}

}
