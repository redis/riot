package com.redislabs.recharge.redis.key;

import java.util.Map;

public class SingleFieldEntityKeyBuilder extends AbstractKeyBuilder {

	private String field;

	public SingleFieldEntityKeyBuilder(String keyspace, String field) {
		super(keyspace);
		this.field = field;
	}

	@Override
	public String getId(Map<String, Object> entity) {
		return getString(entity, field);
	}

}
