package com.redislabs.riot.redis;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.lettusearch.RediSearchClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;

public class LettuSearchConnector extends AbstractLettuceConnector {

	private final Logger log = LoggerFactory.getLogger(LettuSearchConnector.class);

	public LettuSearchConnector(ClientResources resources, RedisURI redisURI,
			GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig) {
		super(resources, redisURI, poolConfig);
	}

	@Override
	protected Supplier<StatefulRedisConnection<String, String>> supplier(AbstractRedisClient redisClient) {
		return ((RediSearchClient) redisClient)::connect;
	}

	@Override
	protected RediSearchClient createClient(ClientResources resources, RedisURI redisURI) {
		log.info("Creating LettuSearch client: {}", redisURI);
		return RediSearchClient.create(resources, redisURI);
	}

}
