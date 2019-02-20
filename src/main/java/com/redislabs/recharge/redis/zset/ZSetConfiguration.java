package com.redislabs.recharge.redis.zset;

import com.redislabs.recharge.redis.CollectionRedisConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
	@EqualsAndHashCode(callSuper = true)
	public class ZSetConfiguration extends CollectionRedisConfiguration {
		private String score;
		private double defaultScore = 1d;
	}

	