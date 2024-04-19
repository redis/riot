package com.redis.riot.redis;

import com.redis.testcontainers.RedisStackContainer;

class ReplicationStackTests extends ReplicationTests {

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
