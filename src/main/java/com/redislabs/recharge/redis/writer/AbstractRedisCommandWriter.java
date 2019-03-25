package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public abstract class AbstractRedisCommandWriter extends AbstractRedisWriter {

	private String keyspace;
	private String[] keys;

	@Override
	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		String id = getValues(record, keys);
		return write(id, record, commands);
	}

	protected abstract RedisFuture<?> write(String id, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

	protected String getKey(String id) {
		if (id == null) {
			return keyspace;
		}
		return join(keyspace, id);
	}

}
