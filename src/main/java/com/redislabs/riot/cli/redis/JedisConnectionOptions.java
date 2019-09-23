package com.redislabs.riot.cli.redis;

import picocli.CommandLine.Option;
import redis.clients.jedis.Protocol;

public class JedisConnectionOptions {

	@Option(names = "--connect-timeout", description = "Connect timeout (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private int connectTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Socket timeout (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

}
