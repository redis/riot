package com.redislabs.riot.redis;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Setter;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionBuilder {

	@Setter
	private String host;
	@Setter
	private int port;
	@Setter
	private String password;
	@Setter
	private int connectionTimeout;
	@Setter
	private int socketTimeout;
	@Setter
	private int database;
	@Setter
	private String clientName;
	@Setter
	private Long commandTimeout;
	@Setter
	private int maxIdle;
	@Setter
	private int minIdle;
	@Setter
	private int maxTotal;
	@Setter
	private long maxWait;

	public RediSearchClient buildLettuceClient() {
		RedisURI redisURI = RedisURI.create(host, port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		if (commandTimeout != null) {
			redisURI.setTimeout(Duration.ofMillis(commandTimeout));
		}
		redisURI.setClientName(clientName);
		redisURI.setDatabase(database);
		return RediSearchClient.create(DefaultClientResources.create(), redisURI);
	}

	public GenericObjectPool<StatefulRediSearchConnection<String, String>> buildLettucePool() {
		RediSearchClient client = buildLettuceClient();
		GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> config = new GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>>();
		GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport
				.createGenericObjectPool(() -> client.connect(), config);
		pool.setMaxTotal(maxTotal);
		pool.setMaxIdle(maxIdle);
		pool.setMinIdle(minIdle);
		pool.setMaxWaitMillis(maxWait);
		return pool;
	}

	public JedisPool buildJedisPool() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(maxIdle);
		config.setMaxIdle(maxTotal);
		config.setMaxWaitMillis(maxWait);
		config.setMinIdle(minIdle);
		return new JedisPool(config, host, port, connectionTimeout, socketTimeout, password, database, clientName);
	}

}
