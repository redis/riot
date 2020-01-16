package com.redislabs.riot.redis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(fluent = true)
public @Data class KeyValue {

	private String key;
	private long ttl;
	private byte[] value;

}
