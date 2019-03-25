package com.redislabs.recharge.redis;

public interface RedisEntry {

	String getKey();

	RedisType getType();

}
