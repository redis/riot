package com.redislabs.riot.test;

import io.lettuce.core.api.sync.BaseRedisCommands;
import redis.clients.jedis.Jedis;

public interface RedisTest {

	void execute(Jedis jedis) throws Exception;

	void execute(BaseRedisCommands<String, String> commands) throws Exception;

}
