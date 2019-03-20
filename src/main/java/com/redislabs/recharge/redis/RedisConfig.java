package com.redislabs.recharge.redis;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

@Configuration
public class RedisConfig {

	@Autowired
	RechargeConfiguration config;
	@Autowired
	GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;
	@Autowired
	@Qualifier("sourceRedisClient")
	RediSearchClient sourceRedisClient;

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources() {
		return DefaultClientResources.create();
	}

	@Primary
	@Bean(name = "sinkRedisClient", destroyMethod = "shutdown")
	RediSearchClient sinkRedisClient(ClientResources clientResources, RedisProperties properties) {
		return client(clientResources, properties);
	}

	@Bean(name = "sourceRedisClient", destroyMethod = "shutdown")
	RediSearchClient sourceRedisClient(ClientResources clientResources,
			@Qualifier("sourceRedisProperties") RedisProperties properties) {
		return client(clientResources, properties);
	}

	@Primary
	@Bean(name = "sinkRedisProperties")
	@ConfigurationProperties(prefix = "")
	RedisProperties sinkRedisProperties() {
		return new RedisProperties();
	}

	@Bean(name = "sourceRedisProperties")
	@ConfigurationProperties(prefix = "source.redis")
	RedisProperties sourceRedisProperties() {
		return new RedisProperties();
	}

	@Primary
	@Bean(name = "sinkRedisPoolProperties")
	@ConfigurationProperties(prefix = "pool")
	Pool sinkRedisPoolProperties() {
		return new Pool();
	}

	@Bean(name = "sourceRedisPoolProperties")
	@ConfigurationProperties(prefix = "source.redis.pool")
	Pool sourceRedisPoolProperties() {
		return new Pool();
	}

	private RediSearchClient client(ClientResources clientResources, RedisProperties redis) {
		RedisURI redisURI = RedisURI.create(redis.getHost(), redis.getPort());
		if (redis.getPassword() != null) {
			redisURI.setPassword(redis.getPassword());
		}
		if (redis.getTimeout() != null) {
			redisURI.setTimeout(redis.getTimeout());
		}
		return RediSearchClient.create(clientResources, redisURI);
	}

	@Primary
	@Bean(name = "sinkRedisConnection", destroyMethod = "close")
	StatefulRediSearchConnection<String, String> sinkRedisConnection(
			@Qualifier("sinkRedisClient") RediSearchClient client) {
		return client.connect();
	}

	@Bean(name = "sourceRedisConnection", destroyMethod = "close")
	StatefulRediSearchConnection<String, String> sourceRedisConnection(
			@Qualifier("sourceRedisClient") RediSearchClient client) {
		return client.connect();
	}

	@Primary
	@Bean(name = "sinkRedisConnectionPool", destroyMethod = "close")
	GenericObjectPool<StatefulRediSearchConnection<String, String>> sinkRedisConnectionPool(
			@Qualifier("sinkRedisClient") RediSearchClient client, Pool poolProperties) {
		return pool(client, poolProperties);
	}

	@Bean(name = "sourceRedisConnectionPool", destroyMethod = "close")
	GenericObjectPool<StatefulRediSearchConnection<String, String>> sourceRedisConnectionPool(
			@Qualifier("sourceRedisClient") RediSearchClient client,
			@Qualifier("sourceRedisPoolProperties") Pool poolProperties) {
		return pool(client, poolProperties);
	}

	private GenericObjectPool<StatefulRediSearchConnection<String, String>> pool(RediSearchClient client,
			Pool poolProperties) {
		GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> config = new GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>>();
		config.setJmxEnabled(false);
		GenericObjectPool<StatefulRediSearchConnection<String, String>> pool = ConnectionPoolSupport
				.createGenericObjectPool(() -> client.connect(), config);
		if (poolProperties != null) {
			pool.setMaxTotal(poolProperties.getMaxActive());
			pool.setMaxIdle(poolProperties.getMaxIdle());
			pool.setMinIdle(poolProperties.getMinIdle());
			if (poolProperties.getMaxWait() != null) {
				pool.setMaxWaitMillis(poolProperties.getMaxWait().toMillis());
			}
		}
		return pool;
	}

	public RedisReader reader() {
		RedisReader reader = new RedisReader();
		reader.setConnection(sourceRedisClient.connect());
		reader.setConfig(config.getReader().getRedis());
		return reader;
	}

	public RedisWriter writer() {
		PipelineRedisWriter writer = config.getWriter().getRedis().writer();
		writer.setFlushall(config.getFlushall());
		writer.setPool(pool);
		return writer;
	}

}
