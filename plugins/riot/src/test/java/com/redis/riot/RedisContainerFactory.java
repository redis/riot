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
		return new RedisStackContainer(RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));
	}

	@SuppressWarnings("resource")
	public static RedisEnterpriseContainer enterprise() {
		return new RedisEnterpriseContainer(RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
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
