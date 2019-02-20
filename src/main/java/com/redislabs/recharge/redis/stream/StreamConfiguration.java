package com.redislabs.recharge.redis.stream;

import com.redislabs.recharge.redis.RedisDataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class StreamConfiguration extends RedisDataStructureConfiguration {
	private boolean approximateTrimming;
	private String id;
	private Long maxlen;
}
