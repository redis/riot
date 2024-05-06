package com.redis.riot.core;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisClientOptions {

	public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = ClientOptions.DEFAULT_PROTOCOL_VERSION;
	public static final boolean DEFAULT_AUTO_RECONNECT = ClientOptions.DEFAULT_AUTO_RECONNECT;

	private boolean cluster;
	private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;
	private SslOptions sslOptions = SslOptions.builder().build();

	public AbstractRedisClient redisClient(RedisURI redisURI) {
		if (cluster) {
			RedisModulesClusterClient client = RedisModulesClusterClient.create(redisURI);
			ClusterClientOptions.Builder options = ClusterClientOptions.builder();
			configure(options);
			client.setOptions(options.build());
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(redisURI);
		ClientOptions.Builder options = ClientOptions.builder();
		configure(options);
		client.setOptions(options.build());
		return client;
	}

	private void configure(ClientOptions.Builder builder) {
		builder.autoReconnect(autoReconnect);
		builder.protocolVersion(protocolVersion);
		builder.sslOptions(sslOptions);
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

	public SslOptions getSslOptions() {
		return sslOptions;
	}

	public void setSslOptions(SslOptions options) {
		this.sslOptions = options;
	}

}
