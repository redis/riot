package com.redislabs.recharge.redis.stream;

import com.redislabs.recharge.redis.RedisCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class StreamConfiguration extends RedisCommandConfiguration {
	private boolean approximateTrimming;
	private String id;
	private Long maxlen;
}
