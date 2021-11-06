package com.redis.riot.redis;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "info", description = "Display INFO command output")
public class InfoCommand extends AbstractRedisCommandCommand {

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) {
		log.info(commands.info());
	}

	@Override
	protected String name() {
		return "info";
	}

}
