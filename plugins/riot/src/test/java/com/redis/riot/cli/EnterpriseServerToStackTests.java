package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class EnterpriseServerToStackTests extends AbstractIntegrationTests {

	private static final RedisEnterpriseServer source = RedisContainerFactory.enterpriseServer();

	private static final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	protected RedisEnterpriseServer getRedisServer() {
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
