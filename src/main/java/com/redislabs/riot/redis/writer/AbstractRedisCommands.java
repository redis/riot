package com.redislabs.riot.redis.writer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public abstract class AbstractRedisCommands implements RedisCommands {

	private ConversionService converter = new DefaultConversionService();

	protected Map<String, String> stringMap(Map<String, Object> item) {
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
}
