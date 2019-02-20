package com.redislabs.recharge.redis.suggest;

import com.redislabs.recharge.redis.RedisDataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SuggestConfiguration extends RedisDataStructureConfiguration {
	private String field;
	private String score;
	private double defaultScore = 1d;
	private boolean increment;
}

