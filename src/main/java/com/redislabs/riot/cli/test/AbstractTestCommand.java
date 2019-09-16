package com.redislabs.riot.cli.test;

import com.redislabs.riot.cli.HelpAwareCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import io.lettuce.core.api.sync.BaseRedisCommands;
import picocli.CommandLine.Mixin;
import redis.clients.jedis.Jedis;

public abstract class AbstractTestCommand extends HelpAwareCommand {

	@Mixin
	private RedisConnectionOptions redis = new RedisConnectionOptions();

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		Object commands = redis.redis();
		if (commands instanceof Jedis) {
			Jedis jedis = (Jedis) commands;
			try {
				run(jedis);
			} finally {
				jedis.close();
			}
		} else {
			run((BaseRedisCommands<String, String>) commands);
		}

	}

	protected abstract void run(Jedis jedis);

	protected abstract void run(BaseRedisCommands<String, String> sync);

}
