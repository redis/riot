package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.ZSetWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zset", description = "Redis sorted set data structure")
public class ZSetWriterCommand extends AbstractCollectionWriterCommand {

	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present", paramLabel = "<float>")
	private double defaultScore = 1d;

	@Override
	protected ZSetWriter collectionWriter() {
		return new ZSetWriter(scoreField, defaultScore);
	}
}
