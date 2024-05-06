package com.redis.riot.cli;

import com.redis.riot.core.RedisClientOptions;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class RedisClientArgs {

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--auto-reconnect", defaultValue = "true", fallbackValue = "true", negatable = true, description = "Automatically reconnect on connection loss. True by default.", hidden = true)
	private boolean autoReconnect = RedisClientOptions.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = RedisClientOptions.DEFAULT_PROTOCOL_VERSION;

	public RedisClientOptions clientOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setCluster(cluster);
		options.setAutoReconnect(autoReconnect);
		options.setProtocolVersion(protocolVersion);
		return options;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public boolean isAutoReconnect() {
		return autoReconnect;
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

}
