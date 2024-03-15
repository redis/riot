package com.redis.riot.core;

import java.io.File;
import java.time.Duration;

import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.SslOptions.Resource;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

public class RedisClientOptions {

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = 6379;
	public static final SslVerifyMode DEFAULT_VERIFY_PEER = SslVerifyMode.FULL;
	public static final Duration DEFAULT_TIMEOUT = RedisURI.DEFAULT_TIMEOUT_DURATION;

	private String uri;
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private String socket;
	private String username;
	private char[] password;
	private Duration timeout = DEFAULT_TIMEOUT;
	private int database;
	private boolean tls;
	private String clientName;
	private SslVerifyMode verifyPeer = DEFAULT_VERIFY_PEER;
	private boolean cluster;
	private Duration metricsStep;
	private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;
	private ProtocolVersion protocolVersion;
	private File keystore;
	private char[] keystorePassword;
	private File truststore;
	private char[] truststorePassword;
	private File keyCert;
	private File key;
	private char[] keyPassword;
	private File trustedCerts;

	public AbstractRedisClient client(RedisURI redisURI) {
		ClientResources resources = clientResources();
		if (cluster) {
			RedisModulesClusterClient client = RedisModulesClusterClient.create(resources, redisURI);
			client.setOptions(clientOptions(ClusterClientOptions.builder()).build());
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(resources, redisURI);
		client.setOptions(clientOptions(ClientOptions.builder()).build());
		return client;
	}

	private <B extends ClientOptions.Builder> B clientOptions(B builder) {
		builder.autoReconnect(autoReconnect);
		builder.sslOptions(sslOptions());
		builder.protocolVersion(protocolVersion);
		return builder;
	}

	public SslOptions sslOptions() {
		Builder ssl = SslOptions.builder();
		if (key != null) {
			ssl.keyManager(keyCert, key, keyPassword);
		}
		if (keystore != null) {
			ssl.keystore(keystore, keystorePassword);
		}
		if (truststore != null) {
			ssl.truststore(Resource.from(truststore), truststorePassword);
		}
		if (trustedCerts != null) {
			ssl.trustManager(trustedCerts);
		}
		return ssl.build();
	}

	private ClientResources clientResources() {
		DefaultClientResources.Builder builder = DefaultClientResources.builder();
		if (metricsStep != null) {
			builder.commandLatencyRecorder(commandLatencyRecorder());
			builder.commandLatencyPublisherOptions(commandLatencyPublisherOptions(metricsStep));
		}
		return builder.build();
	}

	private EventPublisherOptions commandLatencyPublisherOptions(Duration step) {
		return DefaultEventPublisherOptions.builder().eventEmitInterval(step).build();
	}

	private CommandLatencyRecorder commandLatencyRecorder() {
		return CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build());
	}

	public RedisURI redisURI() {
		RedisURI.Builder builder = redisURIBuilder();
		if (database > 0) {
			builder.withDatabase(database);
		}
		if (StringUtils.hasLength(clientName)) {
			builder.withClientName(clientName);
		}
		if (!ObjectUtils.isEmpty(password)) {
			if (StringUtils.hasLength(username)) {
				builder.withAuthentication(username, password);
			} else {
				builder.withPassword(password);
			}
		}
		if (tls) {
			builder.withSsl(tls);
			builder.withVerifyPeer(verifyPeer);
		}
		if (timeout != null) {
			builder.withTimeout(timeout);
		}
		return builder.build();
	}

	private RedisURI.Builder redisURIBuilder() {
		if (StringUtils.hasLength(uri)) {
			return RedisURI.builder(RedisURI.create(uri));
		}
		if (StringUtils.hasLength(socket)) {
			return RedisURI.Builder.socket(socket);
		}
		return RedisURI.Builder.redis(host, port);
	}

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getSocket() {
		return socket;
	}

	public void setSocket(String socket) {
		this.socket = socket;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	public Duration getTimeout() {
		return timeout;
	}

	public void setTimeout(Duration timeout) {
		this.timeout = timeout;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public SslVerifyMode getVerifyPeer() {
		return verifyPeer;
	}

	public void setVerifyPeer(SslVerifyMode mode) {
		this.verifyPeer = mode;
	}

	public File getKeystore() {
		return keystore;
	}

	public void setKeystore(File keystore) {
		this.keystore = keystore;
	}

	public char[] getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(char[] keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public File getTruststore() {
		return truststore;
	}

	public void setTruststore(File truststore) {
		this.truststore = truststore;
	}

	public char[] getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(char[] truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public File getKeyCert() {
		return keyCert;
	}

	public void setKeyCert(File keyCert) {
		this.keyCert = keyCert;
	}

	public File getKey() {
		return key;
	}

	public void setKey(File key) {
		this.key = key;
	}

	public char[] getKeyPassword() {
		return keyPassword;
	}

	public void setKeyPassword(char[] keyPassword) {
		this.keyPassword = keyPassword;
	}

	public File getTrustedCerts() {
		return trustedCerts;
	}

	public void setTrustedCerts(File trustedCerts) {
		this.trustedCerts = trustedCerts;
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

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public Duration getMetricsStep() {
		return metricsStep;
	}

	public void setMetricsStep(Duration metricsStep) {
		this.metricsStep = metricsStep;
	}

}
