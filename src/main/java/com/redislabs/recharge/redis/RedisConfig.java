package com.redislabs.recharge.redis;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
	@Qualifier("readerRedisClient")
	RediSearchClient readerRedisClient;

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources() {
		return DefaultClientResources.create();
	}

	@Primary
	@Bean(name = "writerRedisClient", destroyMethod = "shutdown")
	RediSearchClient writerRedisClient(ClientResources clientResources, RedisProperties writerRedisProperties) {
		return client(clientResources, writerRedisProperties);
	}

	@Bean(name = "readerRedisClient", destroyMethod = "shutdown")
	RediSearchClient readerRedisClient(ClientResources clientResources,
			@Qualifier("readerRedisProperties") RedisProperties readerRedisProperties) {
		return client(clientResources, readerRedisProperties);
	}

	@Primary
	@Bean(name = "writerRedisProperties")
	@ConfigurationProperties(prefix = "")
	RedisProperties writerRedisProperties() {
		return new RedisProperties();
	}

	@Bean(name = "readerRedisProperties")
	@ConfigurationProperties(prefix = "redis.scan")
	RedisProperties readerRedisProperties() {
		return new RedisProperties();
	}

	@Primary
	@Bean(name = "writerRedisPoolProperties")
	@ConfigurationProperties(prefix = "pool")
	Pool writerRedisPoolProperties() {
		return new Pool();
	}

	@Bean(name = "readerRedisPoolProperties")
	@ConfigurationProperties(prefix = "redis.scan.pool")
	Pool readerRedisPoolProperties() {
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
	@Bean(name = "writerRedisConnection", destroyMethod = "close")
	StatefulRediSearchConnection<String, String> writerRedisConnection(
			@Qualifier("writerRedisClient") RediSearchClient writerRedisClient) {
		return writerRedisClient.connect();
	}

	@Bean(name = "readerRedisConnection", destroyMethod = "close")
	@ConditionalOnProperty(name = "redis.scan.host")
	StatefulRediSearchConnection<String, String> readerRedisConnection(
			@Qualifier("readerRedisClient") RediSearchClient readerRedisClient) {
		return readerRedisClient.connect();
	}

	@Primary
	@Bean(name = "writerRedisConnectionPool", destroyMethod = "close")
	GenericObjectPool<StatefulRediSearchConnection<String, String>> writerRedisConnectionPool(
			@Qualifier("writerRedisClient") RediSearchClient writerRedisClient,
			@Qualifier("writerRedisPoolProperties") Pool writerRedisPoolProperties) {
		return pool(writerRedisClient, writerRedisPoolProperties);
	}

	@Bean(name = "readerRedisConnectionPool", destroyMethod = "close")
	GenericObjectPool<StatefulRediSearchConnection<String, String>> readerRedisConnectionPool(
			@Qualifier("readerRedisClient") RediSearchClient readerRedisClient,
			@Qualifier("readerRedisPoolProperties") Pool readerRedisPoolProperties) {
		return pool(readerRedisClient, readerRedisPoolProperties);
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
		reader.setConnection(readerRedisClient.connect());
		reader.setConfig(config.getRedis());
		return reader;
	}

	public RedisWriter writer() {
		PipelineRedisWriter writer = config.getRedis().writer();
		writer.setFlushall(config.getFlushall());
		writer.setPool(pool);
		return writer;
	}

}
