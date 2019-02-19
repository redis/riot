package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HIncrByConfiguration extends AbstractRedisConfiguration {
	private String sourceField;
	private String targetField;
}
