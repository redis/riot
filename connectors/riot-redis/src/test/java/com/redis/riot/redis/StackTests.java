package com.redis.riot.redis;

import com.redis.testcontainers.RedisStackContainer;

class StackTests extends AbstractReplicationTests {

	private static final RedisStackContainer source = RedisContainerFactory.stack();
	private static final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	protected RedisStackContainer getRedisServer() {
		return source;
	}

	@Override
	protected RedisStackContainer getTargetRedisServer() {
		return target;
	}

}
