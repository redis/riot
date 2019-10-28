package com.redislabs.riot.test;

import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

@Slf4j
public class PingTest implements RedisTest {

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
