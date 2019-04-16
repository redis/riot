package com.redislabs.riot.redis;

public interface RedisEntry {

	String getKey();

	RedisType getType();

}
