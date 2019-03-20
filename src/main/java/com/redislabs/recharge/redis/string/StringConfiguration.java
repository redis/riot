package com.redislabs.recharge.redis.string;

import com.redislabs.recharge.redis.RedisCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class StringConfiguration extends RedisCommandConfiguration {
	private Format format = Format.json;
	private XmlStringConfiguration xml;

	public static enum Format {
		xml, json
	}
}
