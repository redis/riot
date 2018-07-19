package com.redislabs.recharge.redis.key;

import java.util.Map;

public class NoFieldKeyBuilder implements KeyBuilder {

	private String keyspace;

	public NoFieldKeyBuilder(String keyspace) {
		this.keyspace = keyspace;
	}

	@Override
	public String getKey(Map<String, Object> record) {
		return keyspace;
	}

	@Override
	public String getId(Map<String, Object> entity) {
		return null;
	}

}
