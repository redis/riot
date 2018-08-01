package com.redislabs.recharge.redis.key;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public abstract class AbstractKeyBuilder implements KeyBuilder {

	private ConversionService conversionService = new DefaultConversionService();

	protected String toString(Object source) {
		return conversionService.convert(source, String.class);
	}

	protected String getString(Map<String, Object> entity, String field) {
		return toString(entity.get(field));
	}

	public static KeyBuilder getKeyBuilder(String[] fields) {
		if (fields == null || fields.length == 0) {
			return new NoFieldKeyBuilder();
		}
		if (fields.length == 1) {
			return new SingleFieldEntityKeyBuilder(fields[0]);
		}
		return new MultipleFieldKeyBuilder(fields);
	}

}
