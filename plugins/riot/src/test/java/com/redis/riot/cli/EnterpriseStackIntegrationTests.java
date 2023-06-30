package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class EnterpriseStackIntegrationTests extends AbstractIntegrationTests {

	private static final RedisServer SOURCE = RedisContainerFactory.enterprise();
	private static final RedisServer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return TARGET;
	}
}
