package com.redislabs.recharge.redis.hash;

import com.redislabs.recharge.redis.RedisDataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HashConfiguration extends RedisDataStructureConfiguration {
	private String[] includeFields;
	private HIncrByConfiguration incrby;
}
