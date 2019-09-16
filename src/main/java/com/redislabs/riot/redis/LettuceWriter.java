package com.redislabs.riot.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class LettuceWriter
		extends AbstractLettuceWriter<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>> {

	private RedisClient client;

	public LettuceWriter(RedisClient client,
			GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig,
			LettuceItemWriter<RedisAsyncCommands<String, String>> writer) {
		super(poolConfig, client::connect, writer);
		this.client = client;
	}

	@Override
	protected void shutdownClient() {
		client.shutdown();
		client.getResources().shutdown();
	}

	protected RedisAsyncCommands<String, String> commands(StatefulRedisConnection<String, String> connection) {
		return connection.async();
	}

}
