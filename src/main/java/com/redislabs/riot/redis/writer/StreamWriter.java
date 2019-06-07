package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;

import lombok.Setter;

@Setter
public class StreamWriter implements RedisItemWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	protected RedisCommands commands;

	private Long maxlen;
	private boolean approximateTrimming;
	private String idField;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		String key = converter.key(item);
		if (idField == null) {
			if (maxlen == null) {
				return commands.xadd(redis, key, item);
			}
			return commands.xadd(redis, key, item, maxlen, approximateTrimming);
		}
		String id = converter.convert(item.remove(idField), String.class);
		if (maxlen == null) {
			return commands.xadd(redis, key, id, item);
		}
		return commands.xadd(redis, key, id, item, maxlen, approximateTrimming);
	}

}