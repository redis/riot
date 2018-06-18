package com.redislabs.recharge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties
public class Recharge {

	private File file = new File();

	private boolean redisearch;

	private String redisearchIndex;

	private Integer maxThreads;

	private int batchSize = 50;

	private Redis redis = new Redis();

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public Redis getRedis() {
		return redis;
	}

	public void setRedis(Redis redis) {
		this.redis = redis;
	}

	public boolean isRedisearch() {
		return redisearch;
	}

	public void setRedisearch(boolean redisearch) {
		this.redisearch = redisearch;
	}

	public String getRedisearchIndex() {
		return redisearchIndex;
	}

	public void setRedisearchIndex(String redisearchIndex) {
		this.redisearchIndex = redisearchIndex;
	}

	public Integer getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(Integer maxThreads) {
		this.maxThreads = maxThreads;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

}
