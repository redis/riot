package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.batch.redis.writer.map.Hmset;

import picocli.CommandLine.Command;

@Command(name = "hmset", description = "Set hash values")
public class HmsetCommand extends AbstractKeyRedisCommand {

	@SuppressWarnings("rawtypes")
	@Override
	protected Hmset keyWriter() {
		return new Hmset();
	}
}
