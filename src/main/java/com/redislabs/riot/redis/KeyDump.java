package com.redislabs.riot.redis;

import lombok.Builder;
import lombok.Data;

@Builder
public @Data class KeyDump {

	private String key;
	private long ttl;
	private byte[] value;

}
