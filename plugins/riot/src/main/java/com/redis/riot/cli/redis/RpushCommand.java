package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.RpushBuilder;

import picocli.CommandLine.Command;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractRedisCollectionCommand {

	@Override
	protected RpushBuilder collectionOperationBuilder() {
		return new RpushBuilder();
	}

}