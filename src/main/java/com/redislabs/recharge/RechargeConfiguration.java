package com.redislabs.recharge;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
public class RechargeConfiguration {

	private Connector connector;
	private int maxThreads = 1;
	private int chunkSize = 50;
	private String host;
	private int port;
	private int timeout;
	private int poolSize;
	private Integer maxItemCount;
	private boolean noOp;

	public Connector getConnector() {
		return connector;
	}

	public void setConnector(Connector connector) {
		this.connector = connector;
	}

	public boolean isNoOp() {
		return noOp;
	}

	public void setNoOp(boolean noOp) {
		this.noOp = noOp;
	}

	public Integer getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(Integer maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

}
