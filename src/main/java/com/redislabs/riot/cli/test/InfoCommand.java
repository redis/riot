package com.redislabs.riot.cli.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import picocli.CommandLine.Command;
import redis.clients.jedis.Jedis;

@Command(name = "info", description = "Execute INFO command")
public class InfoCommand extends AbstractTestCommand {

	private final Logger log = LoggerFactory.getLogger(InfoCommand.class);

	private void log(String info) {
		log.info(info);
	}

	@Override
	protected void run(Jedis jedis) {
		log(jedis.info());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void run(BaseRedisCommands<String, String> sync) {
		log(((RedisServerCommands<String, String>) sync).info());
	}

}
