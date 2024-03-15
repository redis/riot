package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class StackToEnterpriseServerTests extends AbstractIntegrationTests {

	private static final RedisStackContainer source = RedisContainerFactory.stack();
	private static final RedisEnterpriseServer target = RedisContainerFactory.enterpriseServer();

	@Override
	protected RedisStackContainer getRedisServer() {
		return source;
	}

	@Override
	protected RedisEnterpriseServer getTargetRedisServer() {
		return target;
	}

}
