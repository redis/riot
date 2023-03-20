package com.redis.riot;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseSourceIntegrationTests extends AbstractRiotTests {

	@Override
	protected RedisServer getRedisServer() {
		return RedisEnterpriseTargetIntegrationTests.REDIS_ENTERPRISE;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return RedisStackRiotTests.TARGET;
	}
}
