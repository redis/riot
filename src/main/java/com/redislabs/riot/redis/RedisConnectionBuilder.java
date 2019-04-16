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

public class RedisConnectionBuilder {

	@Setter
	private String host;
	@Setter
	private int port;
	@Setter
	private String password;
	@Setter
	private Long timeout;
	@Setter
	private Integer maxIdle;
	@Setter
	private Integer minIdle;
	@Setter
	private Integer maxActive;
	@Setter
	private Long maxWait;

	public RediSearchClient buildClient() {
		RedisURI redisURI = RedisURI.create(host, port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		if (timeout != null) {
			redisURI.setTimeout(Duration.ofMillis(timeout));
		}
		return RediSearchClient.create(DefaultClientResources.create(), redisURI);
	}

	public GenericObjectPool<StatefulRediSearchConnection<String, String>> buildPool() {
		RediSearchClient client = buildClient();
		GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> config = new GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>>();
		GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport
				.createGenericObjectPool(() -> client.connect(), config);
		if (maxActive != null) {
			pool.setMaxTotal(maxActive);
		}
		if (maxIdle != null) {
			pool.setMaxIdle(maxIdle);
		}
		if (minIdle != null) {
			pool.setMinIdle(minIdle);
		}
		if (maxWait != null) {
			pool.setMaxWaitMillis(maxWait);
		}
		return pool;
	}

}
