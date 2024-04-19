package com.redis.riot.core;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public abstract class AbstractRunnable extends AbstractJobRunnable {

	private RedisClientOptions redisClientOptions = new RedisClientOptions();

	private RedisURI redisURI;
	private AbstractRedisClient redisClient;
	private StatefulRedisModulesConnection<String, String> redisConnection;

	@Override
	public void run() {
		redisURI = redisClientOptions.redisURI();
		try {
			redisClient = redisClientOptions.client(redisURI);
			redisConnection = RedisModulesUtils.connection(redisClient);
			super.run();
			redisConnection.close();
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

	protected StatefulRedisModulesConnection<String, String> getRedisConnection() {
		return redisConnection;
	}

}
