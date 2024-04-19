package com.redis.riot.core;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public abstract class AbstractRedisRunnable extends AbstractExecutable {

	private RedisClientOptions redisClientOptions = new RedisClientOptions();

	private RedisURI redisURI;
	private AbstractRedisClient redisClient;

	@Override
	public void execute() throws Exception {
		redisURI = redisClientOptions.redisURI();
		try {
			redisClient = redisClientOptions.client(redisURI);
			super.execute();
		} finally {
			redisClient.close();
			redisClient.getResources().shutdown();
		}
	}

	public RedisClientOptions getRedisClientOptions() {
		return redisClientOptions;
	}

	public void setRedisClientOptions(RedisClientOptions options) {
		this.redisClientOptions = options;
	}

	protected RedisURI getRedisURI() {
		return redisURI;
	}

	protected AbstractRedisClient getRedisClient() {
		return redisClient;
	}

}
