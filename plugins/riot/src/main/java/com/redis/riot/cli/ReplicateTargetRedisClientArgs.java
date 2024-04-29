package com.redis.riot.cli;

import com.redis.riot.cli.RedisReaderArgs.ReadFromEnum;
import com.redis.riot.core.RedisClientOptions;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ReplicateTargetRedisClientArgs {

	@ArgGroup(exclusive = false)
	private ReplicateTargetRedisUriArgs uriArgs = new ReplicateTargetRedisUriArgs();

	@Option(names = "--target-cluster", description = "Enable target cluster mode.")
	private boolean cluster;

	@Option(names = "--target-auto-reconnect", defaultValue = "true", fallbackValue = "true", negatable = true, description = "Automatically reconnect to target on connection loss. True by default.", hidden = true)
	private boolean autoReconnect = true;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion;

	@Option(names = "--target-read-from", description = "Which target cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
	private ReadFromEnum readFrom;

	public ReplicateTargetRedisUriArgs getUriArgs() {
		return uriArgs;
	}

	public void setUriArgs(ReplicateTargetRedisUriArgs uriArgs) {
		this.uriArgs = uriArgs;
	}

	public ReadFromEnum getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFromEnum readFrom) {
		this.readFrom = readFrom;
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

	public RedisClientOptions redisClientOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setAutoReconnect(autoReconnect);
		options.setCluster(cluster);
		options.setProtocolVersion(protocolVersion);
		options.setUriOptions(uriArgs.redisUriOptions());
		return options;
	}

}
