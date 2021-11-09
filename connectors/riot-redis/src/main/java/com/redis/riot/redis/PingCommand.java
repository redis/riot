package com.redis.riot.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends AbstractRedisCommandCommand {

	private static final Logger log = LoggerFactory.getLogger(PingCommand.class);

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		log.info("Received ping reply: " + commands.ping());
	}

	@Override
	protected String name() {
		return "ping";
	}
}
