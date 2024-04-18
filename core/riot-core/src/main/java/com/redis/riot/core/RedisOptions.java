package com.redis.riot.core;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisOptions {

	public static final String DEFAULT_REDIS_HOST = "127.0.0.1";
	public static final int DEFAULT_REDIS_PORT = RedisURI.DEFAULT_REDIS_PORT;
	public static final boolean DEFAULT_AUTO_RECONNECT = ClientOptions.DEFAULT_AUTO_RECONNECT;
	public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = ProtocolVersion.RESP2;

	private RedisURI redisURI = RedisURI.create(DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT);
	private boolean cluster;
	private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;
	private RedisSslOptions sslOptions = new RedisSslOptions();

	public RedisURI getRedisURI() {
		return redisURI;
	}

	public void setRedisURI(RedisURI redisURI) {
		this.redisURI = redisURI;
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

	public RedisSslOptions getSslOptions() {
		return sslOptions;
	}

	public void setSslOptions(RedisSslOptions options) {
		this.sslOptions = options;
	}

	public AbstractRedisClient client() {
		if (cluster) {
			RedisModulesClusterClient client = RedisModulesClusterClient.create(redisURI);
			client.setOptions(clientOptions(ClusterClientOptions.builder()).build());
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(redisURI);
		client.setOptions(clientOptions(ClientOptions.builder()).build());
		return client;
	}

	private <B extends ClientOptions.Builder> B clientOptions(B builder) {
		builder.autoReconnect(autoReconnect);
		builder.sslOptions(sslOptions.sslOptions());
		builder.protocolVersion(protocolVersion);
		return builder;
	}

}
