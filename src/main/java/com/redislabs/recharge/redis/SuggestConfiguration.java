package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SuggestConfiguration extends AbstractRedisConfiguration {
	private String field;
	private String score;
	private double defaultScore = 1d;
	private boolean increment;
}

