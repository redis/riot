package com.redis.riot;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
public class StackREServer extends RiotTests {

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
