package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.writer.AbstractCollectionMapWriter;
import com.redislabs.riot.batch.redis.writer.Zadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zadd", description="Add members with scores to a sorted set")
public class ZaddCommand extends AbstractCollectionRedisCommand {

	@Option(names = "--score", description = "Field to use for sorted set scores", paramLabel = "<field>")
	private String score;
	@Option(names = "--default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<float>")
	private double defaultScore = 1d;

	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractCollectionMapWriter collectionWriter() {
		return new Zadd().defaultScore(defaultScore).scoreField(score);
	}
}
