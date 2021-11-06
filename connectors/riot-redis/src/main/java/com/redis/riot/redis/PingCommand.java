package com.redis.riot.redis;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends AbstractRedisCommandCommand {

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		log.info("Received ping reply: " + commands.ping());
	}

	@Override
	protected String name() {
		return "ping";
	}
}
