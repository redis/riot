package com.redislabs.recharge.redis.hash;

import com.redislabs.recharge.redis.DataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HashConfiguration extends DataStructureConfiguration {
	private String[] includeFields;
	private HIncrByConfiguration incrby;
}
