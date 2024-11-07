package com.redis.riot;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackFileTests extends FileTests {

	private static final RedisStackContainer redis = RedisContainerFactory.stack();

	private static final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	protected RedisStackContainer getRedisServer() {
		return redis;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
