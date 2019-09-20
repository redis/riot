package com.redislabs.riot.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import redis.clients.jedis.Jedis;

public class InfoTest implements RedisTest {

	private final Logger log = LoggerFactory.getLogger(InfoTest.class);

	@Override
	public void execute(Jedis jedis) {
		log.info(jedis.info());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisCommands<String, String> commands) {
		log.info(((RedisServerCommands<String, String>) commands).info());
	}

}
