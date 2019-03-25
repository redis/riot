package com.redislabs.recharge.redis;

import java.time.Duration;

import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;
import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "")
public class RedisProperties {

	/**
	 * Redis server host.
	 */
	private String host = "localhost";

	/**
	 * Login password of the redis server.
	 */
	private String password;

	/**
	 * Redis server port.
	 */
	private int port = 6379;
	/**
	 * Connection timeout.
	 */
	private Duration timeout;

	private Pool pool;

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
	private Long limit;
	private String match;
	private boolean right;
	private String scoreField;
	private double defaultScore = 1d;

	public static enum StringFormat {
		Xml, Json
	}
}
