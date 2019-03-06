package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class CollectionRedisConfiguration extends DataStructureConfiguration {
	private String[] fields = new String[0];
}
