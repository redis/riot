package com.redislabs.recharge.redis.key;

import java.util.Map;

public interface KeyBuilder {

	public static final String KEY_SEPARATOR = ":";

	String getId(Map<String, Object> record);

}
