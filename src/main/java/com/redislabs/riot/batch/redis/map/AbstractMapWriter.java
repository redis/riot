package com.redislabs.riot.batch.redis.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.RedisConverter;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractMapWriter<R> extends AbstractRedisWriter<R, Map<String, Object>> {

	private final ConversionService conversionService = new DefaultConversionService();
	@Setter
	private RedisConverter converter;

	protected <T> T convert(Object source, Class<T> targetType) {
		return conversionService.convert(source, targetType);
	}

	protected String join(Map<String, Object> item, String[] fields) {
		return converter.join(item, fields);
	}

	protected String key(Map<String, Object> item) {
		return converter.key(item);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map stringMap(Map map) {
		map.forEach((k, v) -> map.put(k, conversionService.convert(v, String.class)));
		return map;
	}

	@Override
	public Object write(R redis, Map<String, Object> item) {
		Map<String, Object> flatMap = new HashMap<>();
		item.forEach((k, v) -> flatMap.putAll(flatten(k, v)));
		return write(redis, key(item), flatMap);
	}

	protected abstract Object write(R redis, String key, Map<String, Object> item);

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Object> flatten(String key, Object value) {
		Map<String, Object> flatMap = new HashMap<String, Object>();
		if (value instanceof Map) {
			((Map<String, Object>) value).forEach((k, v) -> {
				flatMap.putAll(flatten(key + "." + k, v));
			});
		} else {
			if (value instanceof Collection) {
				Collection collection = (Collection) value;
				Iterator iterator = collection.iterator();
				int index = 0;
				while (iterator.hasNext()) {
					flatMap.putAll(flatten(key + "[" + index + "]", iterator.next()));
					index++;
				}
			} else {
				flatMap.put(key, value);
			}
		}
		return flatMap;
	}

}
