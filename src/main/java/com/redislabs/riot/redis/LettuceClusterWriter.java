package com.redislabs.riot.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public class LettuceClusterWriter extends
		AbstractLettuceWriter<StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>> {

	private RedisClusterClient client;

	public LettuceClusterWriter(RedisClusterClient client,
			GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> poolConfig,
			LettuceItemWriter<RedisClusterAsyncCommands<String, String>> writer) {
		super(poolConfig, client::connect, writer);
		this.client = client;
	}

	@Override
	protected void shutdownClient() {
		client.shutdown();
		client.getResources().shutdown();
	}

	@Override
	protected RedisClusterAsyncCommands<String, String> commands(
			StatefulRedisClusterConnection<String, String> connection) {
		return connection.async();
	}

}
