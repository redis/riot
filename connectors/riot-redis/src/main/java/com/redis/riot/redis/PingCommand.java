package com.redis.riot.redis;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends AbstractRedisCommandCommand {

	private static final Logger log = Logger.getLogger(PingCommand.class.getName());

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		log.log(Level.INFO, "Received ping reply: {0}", commands.ping());
	}

	@Override
	protected String name() {
		return "ping";
	}
}
