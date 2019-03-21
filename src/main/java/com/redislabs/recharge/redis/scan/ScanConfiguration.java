package com.redislabs.recharge.redis.scan;

import lombok.Data;

@Data
public class ScanConfiguration {
	private Long limit;
	private String match;
}
