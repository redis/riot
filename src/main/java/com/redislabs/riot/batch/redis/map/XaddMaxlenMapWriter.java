package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class XaddMaxlenMapWriter<R> extends AbstractMapWriter<R> {

	@Setter
	private Long maxlen;
	@Setter
	private boolean approximateTrimming;

	@SuppressWarnings("unchecked")
	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		return commands.xadd(redis, key, null, stringMap(item), maxlen, approximateTrimming);
	}

}