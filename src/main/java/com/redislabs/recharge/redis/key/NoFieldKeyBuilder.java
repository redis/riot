package com.redislabs.recharge.redis.key;

import java.util.Map;

public class NoFieldKeyBuilder implements KeyBuilder {

	@Override
	public String getId(Map<String, Object> entity) {
		return null;
	}

}
