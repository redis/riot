package com.redislabs.riot.redis.writer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public abstract class AbstractRedisCommands implements RedisCommands {

	private ConversionService converter = new DefaultConversionService();

	protected Map<String, String> stringMap(Map<String, Object> item) {
		Map<String, String> stringMap = new HashMap<String, String>();
		for (String key : item.keySet()) {
			Object value = item.get(key);
			stringMap.put(key, converter.convert(value, String.class));
		}
		return stringMap;
	}
}
