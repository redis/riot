package com.redislabs.recharge.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "")
public class RedisCommandProperties {

	private RedisType type = RedisType.Hash;
	private String keyspace;
	private String[] keys = new String[0];
	private String field;
	private String incrementField;
	private double defaultIncrement = 1d;
	private String[] fields = new String[0];
	private StringFormat format;
	private String root;
	private String longitudeField;
	private String latitudeField;
	private boolean approximateTrimming;
	private String idField;
	private Long maxlen;
	private boolean right;
	private String scoreField;
	private double defaultScore = 1d;
	private Long limit;
	private String match;

	public static enum StringFormat {
		Xml, Json
	}
}
