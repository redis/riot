package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public abstract class AbstractRedisCollectionWriter extends AbstractRedisDataStructureWriter {

	private String[] fields;

	@Override
	protected RedisFuture<?> write(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		String member = getValues(record, fields);
		return write(key, member, record, commands);
	}

	protected abstract RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

}