package com.redis.riot;

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

public class RedisContext implements AutoCloseable {

	public static final boolean DEFAULT_AUTO_RECONNECT = ClientOptions.DEFAULT_AUTO_RECONNECT;
	public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = ProtocolVersion.RESP2;
	public static final int DEFAULT_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	private final RedisURI uri;
	private final AbstractRedisClient client;
	private final StatefulRedisModulesConnection<String, String> connection;
	private int poolSize = DEFAULT_POOL_SIZE;

	private RedisContext(RedisURI uri, AbstractRedisClient client) {
		this.uri = uri;
		this.client = client;
		this.connection = RedisModulesUtils.connection(client);
	}

	public static RedisContext create(RedisURI uri, boolean cluster, boolean autoReconnect,
			ProtocolVersion protocolVersion, SslOptions sslOptions) {
		RedisModulesClientBuilder clientBuilder = new RedisModulesClientBuilder();
		clientBuilder.cluster(cluster);
		Builder options = cluster ? ClusterClientOptions.builder() : ClientOptions.builder();
		options.autoReconnect(autoReconnect);
		options.protocolVersion(protocolVersion);
		if (sslOptions != null) {
			options.sslOptions(sslOptions);
		}
		clientBuilder.clientOptions(options.build());
		clientBuilder.uri(uri);
		return new RedisContext(uri, clientBuilder.build());
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void configure(RedisItemReader<?, ?, ?> reader) {
		reader.setClient(client);
		reader.setDatabase(uri.getDatabase());
		reader.setPoolSize(poolSize);
	}

	public void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(client);
		writer.setPoolSize(poolSize);
	}

	public RedisURI getUri() {
		return uri;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public StatefulRedisModulesConnection<String, String> getConnection() {
		return connection;
	}

	@Override
	public void close() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
	}

}
