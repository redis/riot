package com.redislabs.riot.redis;

import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;

import lombok.Data;

@Data
public class RedisOptions {

	private String host;
	private String password;
	private int port;
	private long timeout;
	private Pool pool;

}
