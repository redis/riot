package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class StackEnterpriseTests extends AbstractIntegrationTests {

	private static final RedisServer SOURCE = RedisContainerFactory.stack();
	private static final RedisServer TARGET = RedisContainerFactory.enterprise();

	@Override
	protected RedisServer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return TARGET;
	}
}
