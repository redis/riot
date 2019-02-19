package com.redislabs.recharge.redis;

import lombok.Data;

@Data
public class AbstractRedisConfiguration {
	private String keyspace;
	private String[] keys;
}
