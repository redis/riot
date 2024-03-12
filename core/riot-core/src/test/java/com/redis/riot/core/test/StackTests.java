package com.redis.riot.core.test;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.testcontainers.RedisStackContainer;

class StackTests extends AbstractReplicationTests {

	public static final RedisStackContainer SOURCE = RedisContainerFactory.stack();

	public static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisStackContainer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisStackContainer getTargetRedisServer() {
		return TARGET;
	}

	@Override
	protected DataType[] generatorDataTypes() {
		return AbstractTestBase.REDIS_MODULES_GENERATOR_TYPES;
	}

}
