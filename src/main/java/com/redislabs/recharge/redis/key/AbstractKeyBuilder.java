package com.redislabs.recharge.redis.key;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public abstract class AbstractKeyBuilder implements KeyBuilder {

	protected static final String KEY_SEPARATOR = ":";

	private ConversionService conversionService = new DefaultConversionService();
	private String keyspace;

	public AbstractKeyBuilder(String keyspace) {
		this.keyspace = keyspace;
	}

	protected String toString(Object source) {
		return conversionService.convert(source, String.class);
	}

	protected String getString(Map<String, Object> entity, String field) {
		return toString(entity.get(field));
	}

	@Override
	public String getKey(Map<String, Object> entity) {
		return keyspace + KEY_SEPARATOR + getId(entity);
	}
	
	public static KeyBuilder getKeyBuilder(String keyspace, String[] fields) {
		if (fields == null || fields.length == 0) {
			return new NoFieldKeyBuilder(keyspace);
		}
		if (fields.length == 1) {
			return new SingleFieldEntityKeyBuilder(keyspace, fields[0]);
		}
		return new MultipleFieldKeyBuilder(keyspace, fields);
	}

}
