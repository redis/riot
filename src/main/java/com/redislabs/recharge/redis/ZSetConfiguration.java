package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
	@EqualsAndHashCode(callSuper = true)
	public class ZSetConfiguration extends CollectionRedisWriterConfiguration {
		private String score;
		private double defaultScore = 1d;
	}

	