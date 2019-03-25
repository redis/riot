package com.redislabs.recharge;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "")
public class RechargeProperties {

	private boolean meter;
	private int chunkSize = 50;
	private long sleep;
	private int sleepNanos;
	private int partitions = 1;

}
