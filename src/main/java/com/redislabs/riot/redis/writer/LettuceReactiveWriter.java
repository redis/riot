package com.redislabs.riot.redis.writer;

import java.util.List;
import java.util.Map;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

public class LettuceReactiveWriter extends AbstractRedisWriter {

	private StatefulRedisConnection<String, String> connection;
	private LettuceItemWriter writer;

	public LettuceReactiveWriter(StatefulRedisConnection<String, String> connection, LettuceItemWriter writer) {
		this.connection = connection;
		this.writer = writer;
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		if (connection.isOpen()) {
			RedisReactiveCommands<String, String> commands = connection.reactive();
			items.forEach(item -> writer.write(commands, item).block());
		}
	}
}
