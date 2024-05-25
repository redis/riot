package com.redis.riot;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class REServerStack extends RiotTests {

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
