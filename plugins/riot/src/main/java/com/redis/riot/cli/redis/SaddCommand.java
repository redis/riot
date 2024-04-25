package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.SaddBuilder;

import picocli.CommandLine.Command;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionOperationCommand {

	@Override
	protected SaddBuilder collectionOperationBuilder() {
		return new SaddBuilder();
	}

}