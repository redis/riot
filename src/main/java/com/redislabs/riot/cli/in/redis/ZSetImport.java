package com.redislabs.riot.cli.in.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.ZSetWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zset", description = "Sorted set data structure")
public class ZSetImport extends AbstractCollectionImport {

	@Option(names = "--score", description = "Name of the field to use for scores.")
	private String score;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).")
	private Double defaultScore = 1d;

	@Override
	protected AbstractCollectionRedisItemWriter collectionRedisItemWriter() {
		ZSetWriter writer = new ZSetWriter();
		writer.setScoreField(score);
		writer.setDefaultScore(defaultScore);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "zset";
	}

}
