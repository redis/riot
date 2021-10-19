package com.redis.riot.redis;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "info", description = "Display INFO command output")
public class InfoCommand extends AbstractRedisCommandCommand {

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		System.out.println(commands.info());
	}

	@Override
	protected String name() {
		return "info";
	}

}
