package com.redis.riot.cli;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class RedisStackRiotTests extends AbstractRiotTests {

	public static final RedisStackContainer SOURCE = RedisContainerFactory.stack();
	public static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return TARGET;
	}

}
