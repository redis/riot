package com.redislabs.riot.cli.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.sync.BaseRedisCommands;
import picocli.CommandLine.Command;
import redis.clients.jedis.Jedis;

@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends AbstractTestCommand {

	private final Logger log = LoggerFactory.getLogger(PingCommand.class);

	@Override
	protected void run(Jedis jedis) {
		log(jedis.ping());
	}

	@Override
	protected void run(BaseRedisCommands<String, String> sync) {
		log(sync.ping());
	}

	private void log(String ping) {
		log.info("Received ping reply: {}", ping);
	}
}
