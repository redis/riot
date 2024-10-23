package com.redis.riot;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

public interface RedisContainerFactory {

	String ENTERPRISE_TAG = "7.4.6-102";
	String STACK_TAG = "7.2.0-v13";

	static RedisStackContainer stack() {
		return new RedisStackContainer(RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(STACK_TAG));
	}

	@SuppressWarnings("resource")
	static RedisEnterpriseContainer enterprise() {
		return new RedisEnterpriseContainer(RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(ENTERPRISE_TAG))
				.withDatabase(Database.builder().name("BatchTests").memoryMB(50).ossCluster(true)
						.modules(RedisModule.TIMESERIES, RedisModule.JSON, RedisModule.SEARCH).build());
	}

	static RedisEnterpriseServer enterpriseServer() {
		RedisEnterpriseServer server = new RedisEnterpriseServer();
		server.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
				.modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());
		return server;
	}

}
