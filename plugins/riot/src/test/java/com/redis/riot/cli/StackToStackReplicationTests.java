package com.redis.riot.cli;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
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

	@Override
	protected DataType[] generatorDataTypes() {
		return AbstractTestBase.REDIS_MODULES_GENERATOR_TYPES;
	}

}
