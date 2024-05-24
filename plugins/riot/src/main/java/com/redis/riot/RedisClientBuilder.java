package com.redis.riot;

import java.time.Duration;

import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.riot.core.RiotVersion;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisClientBuilder {

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = RedisURI.DEFAULT_REDIS_PORT;
	public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = ClientOptions.DEFAULT_PROTOCOL_VERSION;
	public static final boolean DEFAULT_AUTO_RECONNECT = ClientOptions.DEFAULT_AUTO_RECONNECT;
	public static final SslVerifyMode DEFAULT_VERIFY_MODE = SslVerifyMode.FULL;
	public static final long DEFAULT_TIMEOUT = RedisURI.DEFAULT_TIMEOUT;
	public static final Duration DEFAULT_TIMEOUT_DURATION = RedisURI.DEFAULT_TIMEOUT_DURATION;
	public static final String DEFAULT_CLIENT_NAME = RiotVersion.riotVersion();

	private RedisURI uri;
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private String socket;
	private String username;
	private char[] password;
	private Duration timeout = DEFAULT_TIMEOUT_DURATION;
	private int database;
	private String clientName = DEFAULT_CLIENT_NAME;
	private boolean tls;
	private SslVerifyMode verifyMode = DEFAULT_VERIFY_MODE;
	private boolean cluster;
	private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;
	private SslOptions sslOptions;

	public static class RedisURIClient implements AutoCloseable {

		private final RedisURI uri;
		private final AbstractRedisClient client;

		public RedisURIClient(RedisURI uri, AbstractRedisClient client) {
			this.uri = uri;
			this.client = client;
		}

		public RedisURI getUri() {
			return uri;
		}

		public AbstractRedisClient getClient() {
			return client;
		}

		@Override
		public void close() {
			client.shutdown();
			client.getResources().shutdown();
		}

	}

	public RedisURIClient build() {
		RedisURI redisURI = redisURI();
		AbstractRedisClient redisClient = redisClient(redisURI);
		return new RedisURIClient(redisURI, redisClient);
	}

	private AbstractRedisClient redisClient(RedisURI redisURI) {
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
		builder.protocolVersion(protocolVersion);
		builder.sslOptions(sslOptions);
		return builder;
	}

	private RedisURI.Builder builder() {
		if (uri != null) {
			return RedisURI.builder(uri);
		}
		if (StringUtils.hasLength(socket)) {
			return RedisURI.Builder.socket(socket);
		}
		return RedisURI.Builder.redis(host, port);
	}

	private RedisURI redisURI() {
		RedisURI.Builder builder = builder();
		if (!ObjectUtils.isEmpty(password)) {
			if (StringUtils.hasLength(username)) {
				builder.withAuthentication(username, password);
			} else {
				builder.withPassword(password);
			}
		}
		if (database > 0) {
			builder.withDatabase(database);
		}
		if (tls) {
			builder.withSsl(tls);
		}
		builder.withVerifyPeer(verifyMode);
		builder.withTimeout(timeout);
		RedisURI redisURI = builder.build();
		if (StringUtils.hasLength(clientName) && !StringUtils.hasLength(redisURI.getClientName())) {
			redisURI.setClientName(clientName);
		}
		return redisURI;
	}

	public RedisClientBuilder uri(RedisURI uri) {
		this.uri = uri;
		return this;
	}

	public RedisClientBuilder host(String host) {
		this.host = host;
		return this;
	}

	public RedisClientBuilder port(int port) {
		this.port = port;
		return this;
	}

	public RedisClientBuilder socket(String socket) {
		this.socket = socket;
		return this;
	}

	public RedisClientBuilder username(String username) {
		this.username = username;
		return this;
	}

	public RedisClientBuilder password(char[] password) {
		this.password = password;
		return this;
	}

	public RedisClientBuilder timeout(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	public RedisClientBuilder database(int database) {
		this.database = database;
		return this;
	}

	public RedisClientBuilder clientName(String clientName) {
		this.clientName = clientName;
		return this;
	}

	public RedisClientBuilder tls(boolean tls) {
		this.tls = tls;
		return this;
	}

	public RedisClientBuilder verifyMode(SslVerifyMode verifyMode) {
		this.verifyMode = verifyMode;
		return this;
	}

	public RedisClientBuilder cluster(boolean cluster) {
		this.cluster = cluster;
		return this;
	}

	public RedisClientBuilder autoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
		return this;
	}

	public RedisClientBuilder protocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
		return this;
	}

	public RedisClientBuilder sslOptions(SslOptions sslOptions) {
		this.sslOptions = sslOptions;
		return this;
	}

}
