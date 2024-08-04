package com.redis.riot;

import org.springframework.beans.factory.InitializingBean;

import com.redis.lettucemod.RedisModulesClientBuilder;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;

public class RedisContext implements InitializingBean, AutoCloseable {

	private RedisURI uri;
	private boolean cluster;
	private ClientOptions clientOptions;
	private int poolSize = RedisClientArgs.DEFAULT_POOL_SIZE;

	private AbstractRedisClient client;
	private StatefulRedisModulesConnection<String, String> connection;

	@Override
	public void afterPropertiesSet() throws Exception {
		RedisModulesClientBuilder clientBuilder = new RedisModulesClientBuilder();
		clientBuilder.cluster(cluster);
		clientBuilder.clientOptions(clientOptions);
		clientBuilder.uri(uri);
		client = clientBuilder.build();
		connection = RedisModulesUtils.connection(client);
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

	public ClientOptions getClientOptions() {
		return clientOptions;
	}

	public void setClientOptions(ClientOptions options) {
		this.clientOptions = options;
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
