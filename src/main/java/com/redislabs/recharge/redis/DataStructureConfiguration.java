package com.redislabs.recharge.redis;

import lombok.Data;

@Data
public abstract class DataStructureConfiguration {
	private String keyspace;
	private String[] keys = new String[0];
}
