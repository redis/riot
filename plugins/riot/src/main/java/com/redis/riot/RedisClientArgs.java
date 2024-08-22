package com.redis.riot;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisClientArgs {

	@ArgGroup(exclusive = false, heading = "TLS options%n")
	private SslArgs sslArgs = new SslArgs();

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--auto-reconnect", description = "Automatically reconnect on connection loss. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean autoReconnect = RedisContext.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = RedisContext.DEFAULT_PROTOCOL_VERSION;

	@Option(names = "--pool", description = "Max number of Redis connections in pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
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

	public void setProtocolVersion(ProtocolVersion version) {
		this.protocolVersion = version;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs sslArgs) {
		this.sslArgs = sslArgs;
	}

	@Override
	public String toString() {
		return "RedisClientArgs [sslArgs=" + sslArgs + ", cluster=" + cluster + ", autoReconnect=" + autoReconnect
				+ ", protocolVersion=" + protocolVersion + ", poolSize=" + poolSize + "]";
	}

}
