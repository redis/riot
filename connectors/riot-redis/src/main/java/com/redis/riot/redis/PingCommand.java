package com.redis.riot.redis;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import picocli.CommandLine.Command;

@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends AbstractRedisCommandCommand {

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		System.out.println("Received ping reply: " + commands.ping());
	}

}
