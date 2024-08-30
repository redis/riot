package com.redis.riot;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RedisContext implements AutoCloseable {

	private final RedisURI uri;
	private final AbstractRedisClient client;
	private final StatefulRedisModulesConnection<String, String> connection;

	public RedisContext(RedisURI uri, AbstractRedisClient client) {
		this.uri = uri;
		this.client = client;
		this.connection = RedisModulesUtils.connection(client);
	}

	public void configure(RedisItemReader<?, ?, ?> reader) {
		reader.setClient(client);
		reader.setDatabase(uri.getDatabase());
	}

	public void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(client);
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
