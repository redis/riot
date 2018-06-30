package com.redislabs.recharge.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "batch")
@EnableAutoConfiguration
public class BatchConfiguration {

	private Integer maxThreads;

	private int chunkSize = 50;

	public Integer getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(Integer maxThreads) {
		this.maxThreads = maxThreads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int batchSize) {
		this.chunkSize = batchSize;
	}

}
