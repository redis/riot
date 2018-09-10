package com.redislabs.recharge.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.stereotype.Component;

import io.redisearch.client.Client;

@Component
public class RediSearchClientConfiguration {

	@Autowired
	private RedisProperties properties;

	public Client getClient(String index) {
		return new Client(index, properties.getHost(), properties.getPort(), properties.getTimeout().getNano() * 1000,
				properties.getJedis().getPool().getMaxActive());
	}

}
