package com.redislabs.riot.redis;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;

public class LettuceConnector extends AbstractLettuceConnector {

	private final Logger log = LoggerFactory.getLogger(LettuceConnector.class);

	public LettuceConnector(ClientResources resources, RedisURI redisURI,
			GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig) {
		super(resources, redisURI, poolConfig);
	}

	@Override
	protected Supplier<StatefulRedisConnection<String, String>> supplier(AbstractRedisClient redisClient) {
		return ((RedisClient) redisClient)::connect;
	}

	@Override
	protected RedisClient createClient(ClientResources resources, RedisURI redisURI) {
		log.info("Creating Lettuce client: {} {}", resources, redisURI);
		return RedisClient.create(resources, redisURI);
	}

}
