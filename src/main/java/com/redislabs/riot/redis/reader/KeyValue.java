package com.redislabs.riot.redis.reader;

import lombok.Builder;
import lombok.Data;

@Builder
public @Data class KeyValue {

	private String key;
	private Object value;
	private Type type;

}
