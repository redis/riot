package com.redis.riot.core;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public abstract class AbstractRunnable implements Runnable {

	private RedisClientOptions redisClientOptions = new RedisClientOptions();

	private RedisURI redisURI;
	private AbstractRedisClient redisClient;
	private StatefulRedisModulesConnection<String, String> redisConnection;

	@Override
	public void run() {
		try {
			open();
		} catch (Exception e) {
			throw new ExecutionException("Could not initialize RIOT", e);
		}
		doRun();
		close();
	}

	protected abstract void doRun();

	protected void open() throws Exception {
		redisURI = redisClientOptions.redisURI();
		redisClient = redisClientOptions.client(redisURI);
		redisConnection = RedisModulesUtils.connection(redisClient);
	}

	protected void close() {
		try {
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
