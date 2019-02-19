package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class CollectionRedisWriterConfiguration extends AbstractRedisConfiguration {
	private String[] fields;
}
