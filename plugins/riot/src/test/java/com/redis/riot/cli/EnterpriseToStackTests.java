package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class EnterpriseContainerToStackTests extends AbstractIntegrationTests {

	private static final RedisEnterpriseContainer source = RedisContainerFactory.enterprise();

	private static final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	protected RedisEnterpriseContainer getRedisServer() {
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
