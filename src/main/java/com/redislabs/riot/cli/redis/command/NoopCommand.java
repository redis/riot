package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.AbstractRedisWriter;
import com.redislabs.riot.redis.writer.map.Noop;

import picocli.CommandLine.Command;

@Command(name = "noop", description = "No operation")
public class NoopCommand extends AbstractRedisCommand {

	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractRedisWriter redisWriter() {
		return new Noop();
	}

}
