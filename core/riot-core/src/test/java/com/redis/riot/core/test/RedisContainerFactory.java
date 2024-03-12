package com.redis.riot.core.test;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseContainer;
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
						.modules(RedisModule.TIMESERIES, RedisModule.SEARCH).build());
	}

}
