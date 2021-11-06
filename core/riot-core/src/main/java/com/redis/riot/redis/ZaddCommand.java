package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.support.convert.ScoredValueConverter;
import com.redis.spring.batch.support.operation.Zadd;

import lombok.EqualsAndHashCode;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@EqualsAndHashCode(callSuper = true)
@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractCollectionCommand {

	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault = 1;

	@SuppressWarnings("unchecked")
	@Override
	public Zadd<String, String, Map<String, Object>> operation() {
		return Zadd.key(key())
				.values(new ScoredValueConverter<>(member(), numberExtractor(scoreField, Double.class, scoreDefault)))
				.build();
	}

}
