package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HashConfiguration extends AbstractRedisConfiguration {
	private String[] includeFields;
	private HIncrByConfiguration incrby;
}
