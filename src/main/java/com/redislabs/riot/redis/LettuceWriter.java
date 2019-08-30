package com.redislabs.riot.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

public class LettuceWriter extends AbstractLettuceWriter {

	private RedisClient client;

	public LettuceWriter(RedisClient client,
			GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig, LettuceItemWriter writer) {
		super(poolConfig, client::connect, writer);
		this.client = client;
	}

	@Override
	protected void shutdownClient() {
		client.shutdown();
		client.getResources().shutdown();
	}

}
