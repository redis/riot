package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Setter;

@Setter
public class StreamWriter extends AbstractRedisItemWriter {

	private Long maxlen;
	private boolean approximateTrimming;
	private String idField;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		return commands.xadd(redis, key(item), id(item), item, maxlen, approximateTrimming);
	}

	private String id(Map<String, Object> item) {
		if (idField == null) {
			return null;
		}
		return converter.convert(item.getOrDefault(idField, idField), String.class);
	}

}