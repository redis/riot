package com.redis.riot.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "info", description = "Display INFO command output")
public class InfoCommand extends AbstractRedisCommandCommand {

	private static final Logger log = LoggerFactory.getLogger(InfoCommand.class);

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		log.info(commands.info());
	}

	@Override
	protected String name() {
		return "info";
	}

}
