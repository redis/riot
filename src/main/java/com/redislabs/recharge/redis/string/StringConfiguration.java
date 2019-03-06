package com.redislabs.recharge.redis.string;

import com.redislabs.recharge.redis.DataStructureConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class StringConfiguration extends DataStructureConfiguration {
	private Format format = Format.json;
	private XmlStringConfiguration xml;

	public static enum Format {
		xml, json
	}
}
