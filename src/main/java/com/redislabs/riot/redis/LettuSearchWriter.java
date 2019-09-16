package com.redislabs.riot.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.redisearch.AbstractLettuSearchItemWriter;

public class LettuSearchWriter extends
		AbstractLettuceWriter<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>> {

	private RediSearchClient client;

	public LettuSearchWriter(RediSearchClient client,
			GenericObjectPoolConfig<StatefulRediSearchConnection<String, String>> poolConfig,
			AbstractLettuSearchItemWriter writer) {
		super(poolConfig, client::connect, writer);
		this.client = client;
	}

	@Override
	protected void shutdownClient() {
		client.shutdown();
		client.getResources().shutdown();
	}

	@Override
	protected RediSearchAsyncCommands<String, String> commands(
			StatefulRediSearchConnection<String, String> connection) {
		return connection.async();
	}

}
