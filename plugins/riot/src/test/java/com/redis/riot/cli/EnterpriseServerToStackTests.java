package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

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

}
