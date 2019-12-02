package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.batch.redis.writer.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.writer.map.Noop;

import picocli.CommandLine.Command;

@Command(name = "noop", description = "No operation")
public class NoopCommand extends AbstractRedisCommand {

	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractRedisWriter redisWriter() {
		return new Noop();
	}

}
