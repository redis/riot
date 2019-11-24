package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.writer.Noop;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "noop", description="No operation")
public class NoopCommand extends AbstractRedisCommand {

	@ParentCommand
	private ImportCommand parent;

	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractRedisWriter writer() {
		return new Noop();
	}

}
