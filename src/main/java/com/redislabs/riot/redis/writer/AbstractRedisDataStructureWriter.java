package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public abstract class AbstractRedisDataStructureWriter extends AbstractRedisWriter {

	private String keyspace;
	private String[] keys;

	@Override
	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		String id = getValues(record, keys);
		String key = getKey(id);
		return write(key, record, commands);
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String[] getKeys() {
		return keys;
	}

	protected abstract RedisFuture<?> write(String id, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands);

	private String getKey(String id) {
		if (id == null) {
			return keyspace;
		}
		return keyspace + KEY_SEPARATOR + id;
	}

}
