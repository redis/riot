package com.redis.riot.cli.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class ReplicateCommandContext extends CommandContext {

	private final AbstractRedisClient targetRedisClient;
	private final RedisURI targetRedisURI;

	public ReplicateCommandContext(RedisURI redisURI, AbstractRedisClient client, RedisURI targetRedisURI,
			AbstractRedisClient targetRedisClient) {
		super(redisURI, client);
		this.targetRedisURI = targetRedisURI;
		this.targetRedisClient = targetRedisClient;
	}

	@Override
	public void close() throws Exception {
		targetRedisClient.shutdown();
		targetRedisClient.getResources().shutdown();
		super.close();
	}

	public AbstractRedisClient getTargetRedisClient() {
		return targetRedisClient;
	}

	public RedisURI getTargetRedisURI() {
		return targetRedisURI;
	}

}
