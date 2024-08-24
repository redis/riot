package com.redis.riot;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class TargetRedisClientArgs {

	@Option(names = "--target-cluster", description = "Enable cluster mode for target.")
	private boolean cluster;

	@Option(names = "--target-auto-reconnect", description = "Automatically reconnect to target on connection loss. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean autoReconnect = RedisContext.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = RedisContext.DEFAULT_PROTOCOL_VERSION;

	@Option(names = "--target-pool", description = "Max pool connections used for target Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisContext.DEFAULT_POOL_SIZE;

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

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public String toString() {
		return "TargetRedisClientArgs [cluster=" + cluster + ", autoReconnect=" + autoReconnect + ", protocolVersion="
				+ protocolVersion + ", poolSize=" + poolSize + "]";
	}
}
