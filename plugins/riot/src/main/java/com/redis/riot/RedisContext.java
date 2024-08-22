package com.redis.riot;

import org.springframework.beans.factory.InitializingBean;

import com.redis.lettucemod.RedisModulesClientBuilder;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ClientOptions.Builder;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisContext implements InitializingBean, AutoCloseable {

	public static final boolean DEFAULT_AUTO_RECONNECT = ClientOptions.DEFAULT_AUTO_RECONNECT;
	public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = ClientOptions.DEFAULT_PROTOCOL_VERSION;
	public static final int DEFAULT_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	private RedisURI uri;
	private boolean cluster;
	private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;
	private int poolSize = DEFAULT_POOL_SIZE;
	private SslOptions sslOptions;

	private AbstractRedisClient client;
	private StatefulRedisModulesConnection<String, String> connection;

	@Override
	public void afterPropertiesSet() {
		RedisModulesClientBuilder clientBuilder = new RedisModulesClientBuilder();
		clientBuilder.cluster(cluster);
		clientBuilder.clientOptions(clientOptions());
		clientBuilder.uri(uri);
		client = clientBuilder.build();
		connection = RedisModulesUtils.connection(client);
	}

	private ClientOptions clientOptions() {
		Builder options = clientOptionsBuilder().autoReconnect(autoReconnect).protocolVersion(protocolVersion);
		if (sslOptions != null) {
			options.sslOptions(sslOptions);
		}
		return options.build();
	}

	private ClientOptions.Builder clientOptionsBuilder() {
		if (cluster) {
			return ClusterClientOptions.builder();
		}
		return ClientOptions.builder();
	}

	public void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(client);
		writer.setPoolSize(poolSize);
	}

	public void configure(RedisItemReader<?, ?, ?> reader) {
		reader.setClient(client);
		reader.setPoolSize(poolSize);
		reader.setDatabase(uri.getDatabase());
	}

	public RedisURI getUri() {
		return uri;
	}

	public void setUri(RedisURI uri) {
		this.uri = uri;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public StatefulRedisModulesConnection<String, String> getConnection() {
		return connection;
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

	public void setSslOptions(SslOptions sslOptions) {
		this.sslOptions = sslOptions;
	}

	@Override
	public void close() throws Exception {
		if (connection != null) {
			connection.close();
			connection = null;
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
			client = null;
		}
	}

}
