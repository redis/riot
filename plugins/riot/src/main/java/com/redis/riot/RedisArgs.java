package com.redis.riot;

import java.time.Duration;

import com.redis.lettucemod.RedisModulesClientBuilder;
import com.redis.lettucemod.RedisURIBuilder;
import com.redis.riot.core.RiotVersion;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ClientOptions.Builder;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public interface RedisArgs {

	boolean DEFAULT_CLUSTER = false;
	String DEFAULT_HOST = RedisURIBuilder.DEFAULT_HOST;
	int DEFAULT_PORT = RedisURIBuilder.DEFAULT_PORT;
	long DEFAULT_TIMEOUT = RedisURIBuilder.DEFAULT_TIMEOUT;
	String DEFAULT_CLIENT_NAME = RiotVersion.riotVersion();
	ProtocolVersion DEFAULT_PROTOCOL_VERSION = ProtocolVersion.RESP2;
	int DEFAULT_DATABASE = 0;
	boolean DEFAULT_INSECURE = false;
	boolean DEFAULT_TLS = false;

	default RedisURI getUri() {
		return null;
	}

	default boolean isCluster() {
		return DEFAULT_CLUSTER;
	}

	default ProtocolVersion getProtocolVersion() {
		return DEFAULT_PROTOCOL_VERSION;
	}

	default SslArgs getSslArgs() {
		return null;
	}

	default String getHost() {
		return DEFAULT_HOST;
	}

	default int getPort() {
		return DEFAULT_PORT;
	}

	default String getSocket() {
		return null;
	}

	default String getUsername() {
		return null;
	}

	default char[] getPassword() {
		return null;
	}

	/**
	 * 
	 * @return timeout duration in seconds
	 */
	default long getTimeout() {
		return DEFAULT_TIMEOUT;
	}

	default int getDatabase() {
		return DEFAULT_DATABASE;
	}

	default boolean isTls() {
		return DEFAULT_TLS;
	}

	default boolean isInsecure() {
		return DEFAULT_INSECURE;
	}

	default String getClientName() {
		return DEFAULT_CLIENT_NAME;
	}

	default RedisURI redisURI() {
		return redisURI(getUri());
	}

	default RedisURI redisURI(RedisURI uri) {
		RedisURIBuilder builder = new RedisURIBuilder();
		builder.clientName(getClientName());
		builder.database(getDatabase());
		builder.host(getHost());
		builder.password(getPassword());
		builder.port(getPort());
		builder.socket(getSocket());
		builder.timeout(Duration.ofSeconds(getTimeout()));
		builder.tls(isTls());
		builder.uri(uri);
		builder.username(getUsername());
		if (isInsecure()) {
			builder.verifyMode(SslVerifyMode.NONE);
		}
		return builder.build();
	}

	default RedisContext redisContext() {
		return redisContext(getUri());
	}

	default RedisContext redisContext(RedisURI uri) {
		return redisContext(uri, getSslArgs());
	}

	default RedisContext redisContext(RedisURI uri, SslArgs sslArgs) {
		RedisURI finalUri = redisURI(uri);
		RedisModulesClientBuilder clientBuilder = new RedisModulesClientBuilder();
		clientBuilder.cluster(isCluster());
		Builder options = isCluster() ? ClusterClientOptions.builder() : ClientOptions.builder();
		options.protocolVersion(getProtocolVersion());
		if (sslArgs != null) {
			options.sslOptions(sslArgs.sslOptions());
		}
		clientBuilder.clientOptions(options.build());
		clientBuilder.uri(finalUri);
		return new RedisContext(finalUri, clientBuilder.build());
	}

}
