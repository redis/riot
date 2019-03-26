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

}
