package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;

import com.redis.spring.batch.convert.ScoredValueConverter;
import com.redis.spring.batch.writer.operation.Zadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractCollectionCommand {

	public static final double DEFAULT_SCORE = 1;

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private Optional<String> score = Optional.empty();
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double defaultScore = DEFAULT_SCORE;

	@Override
	public Zadd<String, String, Map<String, Object>> operation() {
		return new Zadd<>(key(), new ScoredValueConverter<>(member(), doubleExtractor(score, defaultScore)));
	}

}
