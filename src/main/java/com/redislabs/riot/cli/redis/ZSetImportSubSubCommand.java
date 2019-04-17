package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.ZSetWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zset", description = "Sorted set data structure")
public class ZSetImportSubSubCommand extends AbstractRedisCollectionImportSubSubCommand {

	@Option(names = "--score-field", description = "Name of the field to use for scores.")
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).")
	private Double defaultScore = 1d;

	@Override
	protected ZSetWriter doCreateWriter() {
		ZSetWriter writer = new ZSetWriter();
		writer.setScoreField(scoreField);
		writer.setDefaultScore(defaultScore);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "zset";
	}

}
