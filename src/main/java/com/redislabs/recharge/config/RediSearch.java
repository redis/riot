package com.redislabs.recharge.config;

import redis.clients.jedis.Protocol;

public class RediSearch {

	private boolean enabled;

	private String index;

	private boolean cluster;

	/**
	 * RediSearch server host.
	 */
	private String host = Protocol.DEFAULT_HOST;

	/**
	 * Login password of the RediSearch server.
	 */
	private String password;

	/**
	 * RediSearch server port.
	 */
	private int port = Protocol.DEFAULT_PORT;

	private int timeout = Protocol.DEFAULT_TIMEOUT;

	private int poolSize = 100;

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

	public int getPort() {
		return this.port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String redisearchIndex) {
		this.index = redisearchIndex;
	}

}
