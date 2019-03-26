package com.redislabs.recharge.redis;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class RedisConfig {

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources() {
		return DefaultClientResources.create();
	}

	@Bean(destroyMethod = "shutdown")
	RediSearchClient redisClient(ClientResources resources, RedisProperties redis) {
		RedisURI redisURI = RedisURI.create(redis.getHost(), redis.getPort());
		if (redis.getPassword() != null) {
			redisURI.setPassword(redis.getPassword());
		}
		if (redis.getTimeout() != null) {
			redisURI.setTimeout(redis.getTimeout());
		}
		return RediSearchClient.create(resources, redisURI);
	}

	@Bean(destroyMethod = "close")
	StatefulRediSearchConnection<String, String> redisConnection(RediSearchClient client) {
		return client.connect();
	}

	@Bean(destroyMethod = "close")
	GenericObjectPool<StatefulRediSearchConnection<String, String>> redisConnectionPool(RediSearchClient client,
			RedisProperties props) {
		GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> config = new GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>>();
		config.setJmxEnabled(false);
		GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport
				.createGenericObjectPool(() -> client.connect(), config);
		if (props.getPool() != null) {
			pool.setMaxTotal(props.getPool().getMaxActive());
			pool.setMaxIdle(props.getPool().getMaxIdle());
			pool.setMinIdle(props.getPool().getMinIdle());
			if (props.getPool().getMaxWait() != null) {
				pool.setMaxWaitMillis(props.getPool().getMaxWait().toMillis());
			}
		}
		return pool;
	}

}
