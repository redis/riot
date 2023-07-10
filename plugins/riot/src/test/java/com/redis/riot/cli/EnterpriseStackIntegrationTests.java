package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.riot.cli.common.AbstractExportCommand;
import com.redis.testcontainers.RedisServer;

import picocli.CommandLine.ParseResult;

@EnabledOnOs(OS.LINUX)
class EnterpriseStackIntegrationTests extends AbstractIntegrationTests {

	private static final RedisServer SOURCE = RedisContainerFactory.enterprise();
	private static final RedisServer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return TARGET;
	}

	@Override
	protected void configureSubcommand(ParseResult sub) {
		super.configureSubcommand(sub);
		Object commandObject = sub.commandSpec().commandLine().getCommand();
		if (commandObject instanceof AbstractExportCommand) {
			AbstractExportCommand command = (AbstractExportCommand) commandObject;
			command.getReaderOptions().setMemLimit(0);
		}
	}
}
