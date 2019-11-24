package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.writer.Sadd;

import picocli.CommandLine.Command;

@Command(name = "sadd", description="Add members to a set")
public class SaddCommand extends AbstractCollectionRedisCommand {

	@SuppressWarnings("rawtypes")
	@Override
	protected Sadd collectionWriter() {
		return new Sadd();
	}

}
