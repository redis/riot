
package com.redislabs.riot.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public class RedisConverter {

	private ConversionService converter = new DefaultConversionService();

	private String separator;
	private String keyspace;
	private String[] keys;

	public RedisConverter(String separator, String keyspace, String[] keys) {
		this.separator = separator;
		this.keyspace = keyspace;
		this.keys = keys;
	}

	public String id(Map<String, Object> item) {
		return join(item, keys);
	}

	public String key(Map<String, Object> item) {
		return key(id(item));
	}

	public String key(String id) {
		if (id == null) {
			return keyspace;
		}
		if (keyspace == null) {
			return id;
		}
		return keyspace + separator + id;
	}

	public String join(Map<String, Object> item, String[] fields) {
		if (fields.length == 0) {
			return null;
		}
		StringJoiner joiner = new StringJoiner(separator);
		for (String field : fields) {
			joiner.add(converter.convert(item.get(field), String.class));
		}
		return joiner.toString();
	}

	public <T> T convert(Object source, Class<T> targetType) {
		return converter.convert(source, targetType);
	}

	public Map<String, String> stringMap(Map<String, Object> item) {
		Map<String, String> stringMap = new HashMap<String, String>();
		item.forEach((k, v) -> put(stringMap, k, v));
		return stringMap;
	}

	private void put(Map<String, String> map, String key, Object value) {
		if (value == null) {
			return;
		}
		if (value instanceof Map) {
			((Map<?, ?>) value).forEach((k, v) -> put(map, key + "." + converter.convert(k, String.class), v));
		} else {
			map.put(key, converter.convert(value, String.class));
		}
	}

	@Override
	public String toString() {
		if (keyspace == null) {
			return keysDescription();
		}
		if (keys.length > 0) {
			return keyspace + separator + keysDescription();
		}
		return keyspace;
	}

	private String keysDescription() {
		return String.join(separator, wrap(keys));
	}

	private String[] wrap(String[] fields) {
		String[] results = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			results[index] = "<" + fields[index] + ">";
		}
		return results;
	}

}
