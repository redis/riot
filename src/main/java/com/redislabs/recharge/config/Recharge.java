package com.redislabs.recharge.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
public class Recharge {

	private File file = new File();

	private Generator generator = new Generator();

	private Integer maxThreads;

	private int batchSize = 50;

	private Key key = new Key();

	private RediSearch redisearch = new RediSearch();

	private Integer maxItemCount;

	public Integer getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(Integer maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

	public Generator getGenerator() {
		return generator;
	}

	public void setGenerator(Generator generator) {
		this.generator = generator;
	}

	public RediSearch getRedisearch() {
		return redisearch;
	}

	public void setRedisearch(RediSearch rediSearch) {
		this.redisearch = rediSearch;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public Key getKey() {
		return key;
	}

	public void setKey(Key redis) {
		this.key = redis;
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
