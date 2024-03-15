package com.redis.riot.file;

import com.redis.testcontainers.RedisStackContainer;

class StackFileTests extends AbstractFileTests {

	private static final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected RedisStackContainer getRedisServer() {
		return redis;
	}

}
