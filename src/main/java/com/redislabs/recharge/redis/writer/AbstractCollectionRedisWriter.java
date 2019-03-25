package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public abstract class AbstractCollectionRedisWriter extends AbstractRedisCommandWriter {

	private String[] fields;

	@Override
	protected RedisFuture<?> write(String id, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		String member = getValues(record, fields);
		return write(getKey(id), member, record, commands);
	}

	protected abstract RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

}