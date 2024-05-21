package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.ZaddSupplier;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractCollectionOperationCommand {

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private String scoreField;

	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double defaultScore = ZaddSupplier.DEFAULT_SCORE;

	@Override
	protected ZaddSupplier collectionOperationBuilder() {
		ZaddSupplier supplier = new ZaddSupplier();
		supplier.setScoreField(scoreField);
		supplier.setDefaultScore(defaultScore);
		return supplier;
	}

}