package com.redis.riot.cli;

import com.redis.testcontainers.RedisStackContainer;

class StackToStackReplicationTests extends AbstractReplicationTests {

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
