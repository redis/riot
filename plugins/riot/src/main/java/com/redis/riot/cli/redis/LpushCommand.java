package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.LpushBuilder;

import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractRedisCollectionCommand {

	@Override
	protected LpushBuilder collectionOperationBuilder() {
		return new LpushBuilder();
	}

}