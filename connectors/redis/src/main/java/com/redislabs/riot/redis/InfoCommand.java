package com.redislabs.riot.redis;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import picocli.CommandLine.Command;

@Command(name = "info", aliases = { "i" }, description = "Display INFO command output")
public class InfoCommand extends AbstractRedisCommand {

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisCommands<String, String> commands) {
		System.out.println(((RedisServerCommands<String, String>) commands).info());
	}

}
