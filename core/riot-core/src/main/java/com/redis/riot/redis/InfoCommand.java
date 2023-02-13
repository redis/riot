package com.redis.riot.redis;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "test-info", description = "Display output of Redis info command")
public class InfoCommand extends AbstractRedisCommand {

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		System.out.println(commands.info());
	}

	@Override
	protected String name() {
		return "info";
	}

}
