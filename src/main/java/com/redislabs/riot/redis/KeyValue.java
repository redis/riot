package com.redislabs.riot.redis;

import lombok.Getter;
import lombok.Setter;

public class KeyValue {

	private @Getter @Setter String key;
	private @Getter @Setter long ttl;
	private @Getter @Setter byte[] value;

}
