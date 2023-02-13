package com.redis.riot.redis;

import java.text.MessageFormat;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import picocli.CommandLine.Command;

@Command(name = "test-ping", description = "Execute Redis ping command")
public class PingCommand extends AbstractRedisCommand {

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		System.out.println(MessageFormat.format("Received PING reply: {0}", commands.ping()));
	}

	@Override
	protected String name() {
		return "ping";
	}
}
