package com.redislabs.recharge.redis.hash;

import com.redislabs.recharge.redis.RedisDataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HIncrByConfiguration extends RedisDataStructureConfiguration {
	private String sourceField;
	private String targetField;
}
