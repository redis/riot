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
		String key = key(item);
		if (idField == null) {
			if (maxlen == null) {
				return commands.xadd(redis, key, item);
			}
			return commands.xadd(redis, key, item, maxlen, approximateTrimming);
		}
		String id = converter.convert(item.getOrDefault(idField, idField), String.class);
		if (maxlen == null) {
			return commands.xadd(redis, key, id, item);
		}
		return commands.xadd(redis, key, id, item, maxlen, approximateTrimming);
	}

}