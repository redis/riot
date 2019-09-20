package com.redislabs.riot.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.sync.BaseRedisCommands;
import redis.clients.jedis.Jedis;

public class PingTest implements RedisTest {

	private final Logger log = LoggerFactory.getLogger(PingTest.class);

	@Override
	public void execute(Jedis jedis) {
		log(jedis.ping());
	}

	@Override
	public void execute(BaseRedisCommands<String, String> commands) {
		log(commands.ping());
	}

	private void log(String response) {
		log.info("Received ping reply: {}", response);
	}

}
