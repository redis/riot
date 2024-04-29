package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class EnterpriseContainerStackReplicationTests extends ReplicationTests {

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

}
