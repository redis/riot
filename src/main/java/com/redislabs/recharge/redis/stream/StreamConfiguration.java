package com.redislabs.recharge.redis.stream;

import com.redislabs.recharge.redis.DataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class StreamConfiguration extends DataStructureConfiguration {
	private boolean approximateTrimming;
	private String id;
	private Long maxlen;
}
