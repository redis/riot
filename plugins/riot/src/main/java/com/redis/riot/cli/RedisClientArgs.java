package com.redis.riot.cli;

import com.redis.riot.core.RedisClientOptions;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisClientArgs {

	@ArgGroup(exclusive = false)
	private RedisURIArgs uriArgs = new RedisURIArgs();

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--auto-reconnect", defaultValue = "true", fallbackValue = "true", negatable = true, description = "Automatically reconnect on connection loss. True by default.", hidden = true)
	private boolean autoReconnect = true;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion;

	@ArgGroup(exclusive = false)
	private SslArgs sslArgs = new SslArgs();

	public RedisClientOptions redisClientOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setRedisURI(uriArgs.redisURI());
		options.setCluster(cluster);
		options.setOptions(clientOptions());
		return options;
	}

	private ClientOptions clientOptions() {
		if (cluster) {
			ClusterClientOptions.Builder options = ClusterClientOptions.builder();
			configure(options);
			return options.build();
		}
		ClientOptions.Builder options = ClientOptions.builder();
		configure(options);
		return options.build();
	}

	private void configure(ClientOptions.Builder builder) {
		builder.autoReconnect(autoReconnect);
		builder.protocolVersion(protocolVersion);
		builder.sslOptions(sslArgs.sslOptions());
	}

	public RedisURIArgs getUriArgs() {
		return uriArgs;
	}

	public void setUriArgs(RedisURIArgs args) {
		this.uriArgs = args;
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

	public void setProtocolVersion(ProtocolVersion version) {
		this.protocolVersion = version;
	}

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs args) {
		this.sslArgs = args;
	}

}