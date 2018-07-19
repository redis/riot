package com.redislabs.recharge.redis.key;

import java.util.Map;

public interface KeyBuilder {

	String getId(Map<String, Object> record);

	String getKey(Map<String, Object> record);

}
