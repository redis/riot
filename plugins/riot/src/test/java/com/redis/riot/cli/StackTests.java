package com.redis.riot.cli;

import org.junit.jupiter.api.Test;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackTests extends AbstractIntegrationTests {

	public static final RedisStackContainer SOURCE = RedisContainerFactory.stack();
	public static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return TARGET;
	}

	@Test
	void replicateLiveMultiThreaded() throws Exception {
		runLiveReplication("replicate-live-threads");
	}

}
