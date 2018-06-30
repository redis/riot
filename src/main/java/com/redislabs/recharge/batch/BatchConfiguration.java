package com.redislabs.recharge.batch;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "batch")
@EnableAutoConfiguration
public class BatchConfiguration {

	private Integer maxThreads;

	private int size = 50;

	public Integer getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(Integer maxThreads) {
		this.maxThreads = maxThreads;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int batchSize) {
		this.size = batchSize;
	}

}
