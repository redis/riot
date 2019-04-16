package com.redislabs.riot.redis;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisConfig {

	public RediSearchClient client(String host, int port, String password, Long timeout) {
		RedisURI redisURI = RedisURI.create(host, port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		if (timeout != null) {
			redisURI.setTimeout(Duration.ofMillis(timeout));
		}
		return RediSearchClient.create(DefaultClientResources.create(), redisURI);
	}

	public GenericObjectPool<StatefulRediSearchConnection<String, String>> pool(RediSearchClient client, Pool options) {
		GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> config = new GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>>();
		GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport
				.createGenericObjectPool(() -> client.connect(), config);
		if (options != null) {
			pool.setMaxTotal(options.getMaxActive());
			pool.setMaxIdle(options.getMaxIdle());
			pool.setMinIdle(options.getMinIdle());
			if (options.getMaxWait() != null) {
				pool.setMaxWaitMillis(options.getMaxWait().toMillis());
			}
		}
		return pool;
	}

}
