
package com.redislabs.riot.redis;

import java.util.Map;
import java.util.StringJoiner;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import lombok.Getter;
import lombok.Setter;

public class RedisConverter {

	private ConversionService converter = new DefaultConversionService();
	@Getter
	@Setter
	private String keyspace;
	@Getter
	@Setter
	private String[] keys;
	@Setter
	private String separator;

	public String id(Map<String, Object> item) {
		return joinFields(item, keys);
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

	public String joinFields(Map<String, Object> item, String[] fields) {
		if (fields == null || fields.length == 0) {
			return null;
		}
		StringJoiner joiner = new StringJoiner(separator);
		for (String field : fields) {
			joiner.add(converter.convert(item.remove(field), String.class));
		}
		return joiner.toString();
	}

	public <T> T convert(Object source, Class<T> targetType) {
		return converter.convert(source, targetType);
	}

}
