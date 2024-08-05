package com.redis.riot;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

public class RedisContainerFactory {

	private RedisContainerFactory() {
	}

	public static RedisStackContainer stack() {
		return new RedisStackContainer(RedisStackContainer.DEFAULT_IMAGE_NAME.withTag("7.2.0-v11"));
	}

	@SuppressWarnings("resource")
	public static RedisEnterpriseContainer enterprise() {
		return new RedisEnterpriseContainer(
				RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG))
				.withDatabase(Database.builder().name("BatchTests").memoryMB(50).ossCluster(true)
						.modules(RedisModule.TIMESERIES, RedisModule.JSON, RedisModule.SEARCH).build());
	}

	public static RedisEnterpriseServer enterpriseServer() {
		RedisEnterpriseServer server = new RedisEnterpriseServer();
		server.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
				.modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());
		return server;
	}

}
