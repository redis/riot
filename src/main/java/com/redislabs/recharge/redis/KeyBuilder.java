package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.recharge.config.KeyConfiguration;

public class KeyBuilder {

	private ConversionService conversionService = new DefaultConversionService();
	private KeyConfiguration config;

	public KeyBuilder(KeyConfiguration config) {
		this.config = config;
	}

	public String getKey(Map<String, Object> map) {
		return getPrefix() + config.getSeparator() + getId(map);
	}

	private String getPrefix() {
		if (config.getPrefix() == null) {
			return config.getFields()[0];
		}
		return config.getPrefix();
	}

	public String getId(Map<String, Object> map) {
		return getId(getKeyFieldValues(map));
	}

	private Object[] getKeyFieldValues(Map<String, Object> map) {
		if (config.getFields() == null || config.getFields().length == 0) {
			return map.values().toArray();
		}
		Object[] values = new Object[config.getFields().length];
		for (int index = 0; index < values.length; index++) {
			values[index] = map.get(config.getFields()[index]);
		}
		return values;
	}

	private String getId(Object[] values) {
		String id = toString(values[0]);
		for (int index = 1; index < values.length; index++) {
			id += config.getSeparator() + toString(values[index]);
		}
		return id;
	}

	private String toString(Object value) {
		return conversionService.convert(value, String.class);
	}
}
