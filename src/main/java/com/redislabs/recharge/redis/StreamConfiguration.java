package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
	@EqualsAndHashCode(callSuper = true)
	public class StreamConfiguration extends AbstractRedisConfiguration {
		private boolean approximateTrimming;
		private String id;
		private Long maxlen;
	}

	