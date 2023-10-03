package com.redis.riot.core.test;

import org.springframework.util.unit.DataSize;

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
				.withDatabase(Database.name("BatchTests").memory(DataSize.ofMegabytes(50)).ossCluster(true)
						.modules(RedisModule.JSON, RedisModule.TIMESERIES, RedisModule.SEARCH).build());
	}

}
