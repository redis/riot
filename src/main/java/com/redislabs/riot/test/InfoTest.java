package com.redislabs.riot.test;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

@Slf4j
public class InfoTest implements RedisTest {

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
