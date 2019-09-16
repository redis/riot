package com.redislabs.riot.cli.redis;

public class RedisEndpoint {

	private static final String SEPARATOR = ":";
	private String host;
	private int port;

	public RedisEndpoint(String endpoint) {
		String[] split = endpoint.split(SEPARATOR);
		this.host = split[0];
		this.port = Integer.parseInt(split[1]);
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return host + SEPARATOR + port;
	}

}
