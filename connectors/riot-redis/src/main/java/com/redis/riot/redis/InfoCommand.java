package com.redis.riot.redis;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "info", description = "Display INFO command output")
public class InfoCommand extends AbstractRedisCommand {

	private static final Logger log = Logger.getLogger(InfoCommand.class.getName());

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		if (log.isLoggable(Level.INFO)) {
			log.info(commands.info());
		}
	}

	@Override
	protected String name() {
		return "info";
	}

}
