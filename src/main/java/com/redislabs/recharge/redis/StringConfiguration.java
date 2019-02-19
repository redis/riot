package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
	@EqualsAndHashCode(callSuper = true)
	public class StringConfiguration extends AbstractRedisConfiguration {
		private XmlConfiguration xml;
		private JsonConfiguration json;
	}

	