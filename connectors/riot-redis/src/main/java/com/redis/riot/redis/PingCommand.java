package com.redis.riot.redis;

import io.lettuce.core.api.sync.BaseRedisCommands;
import picocli.CommandLine.Command;

@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends AbstractUtilityCommand {

	@Override
	protected void execute(BaseRedisCommands<String, String> commands) {
		System.out.println("Received ping reply: " + commands.ping());
	}

}
