package com.redislabs.recharge.redis;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "redisearch")
@EnableAutoConfiguration
public class RediSearchConfiguration {

	private String index;

	private boolean cluster;

	/**
	 * RediSearch server host.
	 */
	private String host;

	/**
	 * Login password of the RediSearch server.
	 */
	private String password;

	/**
	 * RediSearch server port.
	 */
	private Integer port;

	private Integer timeout;

	private Integer poolSize = 100;

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Integer getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(Integer poolSize) {
		this.poolSize = poolSize;
	}

	public Integer getTimeout() {
		return timeout;
	}

	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isEnabled() {
		return index != null && index.length() > 0;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String redisearchIndex) {
		this.index = redisearchIndex;
	}

}
