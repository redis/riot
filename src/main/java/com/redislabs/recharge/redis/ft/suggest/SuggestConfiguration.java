package com.redislabs.recharge.redis.ft.suggest;

import com.redislabs.recharge.redis.ft.RediSearchCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SuggestConfiguration extends RediSearchCommandConfiguration {
	private String field;
	private String score;
	private double defaultScore = 1d;
	private boolean increment;
}
