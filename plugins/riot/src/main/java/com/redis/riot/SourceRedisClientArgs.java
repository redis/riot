package com.redis.riot;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class SourceRedisClientArgs {

	@Option(names = "--source-cluster", description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--source-auto-reconnect", description = "Automatically reconnect on connection loss. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean autoReconnect = RedisContext.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--source-resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = RedisContext.DEFAULT_PROTOCOL_VERSION;

	@Option(names = "--source-pool", description = "Max pool connections used for source Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
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
		return "SourceRedisClientArgs [cluster=" + cluster + ", autoReconnect=" + autoReconnect + ", protocolVersion="
				+ protocolVersion + ", poolSize=" + poolSize + "]";
	}

}
