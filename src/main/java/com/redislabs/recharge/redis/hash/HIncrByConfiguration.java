package com.redislabs.recharge.redis.hash;

import com.redislabs.recharge.redis.RedisCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HIncrByConfiguration extends RedisCommandConfiguration {
	private String sourceField;
	private String targetField;
}
