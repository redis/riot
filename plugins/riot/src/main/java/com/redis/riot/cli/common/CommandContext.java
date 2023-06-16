package com.redis.riot.cli.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class CommandContext implements AutoCloseable {

	private final AbstractRedisClient redisClient;
	private final RedisURI redisURI;

	public CommandContext(RedisURI redisURI, AbstractRedisClient client) {
		this.redisURI = redisURI;
		this.redisClient = client;
	}

	public AbstractRedisClient getRedisClient() {
		return redisClient;
	}

	public RedisURI getRedisURI() {
		return redisURI;
	}

	@Override
	public void close() throws Exception {
		redisClient.shutdown();
		redisClient.getResources().shutdown();
	}

}
