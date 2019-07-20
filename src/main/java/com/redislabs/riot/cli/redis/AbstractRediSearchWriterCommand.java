package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redisearch.AbstractLettuSearchItemWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "search", description = "RediSearch index")
public abstract class AbstractRediSearchWriterCommand extends AbstractDataStructureWriterCommand {

	@Parameters(paramLabel = "INDEX", description = "Name of the RediSearch index")
	protected String index;
	@Option(names = "--default-score", description = "Default score to use when score field is not present", paramLabel = "<score>")
	protected double defaultScore = 1d;
	@Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
	protected String payloadField;
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	protected String scoreField;

	@Override
	protected String description() {
		return "RediSearch " + commandSpec.name() + " index \"" + index + "\"";
	}

	@Override
	protected AbstractRedisItemWriter writer() {
		AbstractLettuSearchItemWriter writer = rediSearchWriter();
		writer.setIndex(index);
		writer.setScoreField(scoreField);
		writer.setDefaultScore(defaultScore);
		return writer;
	}

	protected abstract AbstractLettuSearchItemWriter rediSearchWriter();

}
