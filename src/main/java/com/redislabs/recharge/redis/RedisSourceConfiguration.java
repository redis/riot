package com.redislabs.recharge.redis;

import lombok.Data;

@Data
public class RedisSourceConfiguration {
	private Long limit;
	private String match;
}
