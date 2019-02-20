package com.redislabs.recharge.redis.string;

import com.redislabs.recharge.redis.RedisDataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
	@EqualsAndHashCode(callSuper = true)
	public class StringConfiguration extends RedisDataStructureConfiguration {
		private XmlStringConfiguration xml;
		private JsonStringConfiguration json;
	}

	