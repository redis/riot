package com.redislabs.recharge.redis;

import lombok.Data;

@Data
public class ScanConfiguration {

	private Long limit;
	private String match;

}
