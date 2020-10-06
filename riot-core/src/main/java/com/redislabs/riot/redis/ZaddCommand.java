package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "zadd")
public class ZaddCommand extends AbstractCollectionCommand {

	@CommandLine.Option(names = "--zset-score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@CommandLine.Option(names = "--zset-default", defaultValue = "1", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault;

	@Override
	protected Zadd<String, String, Map<String, Object>> collectionWriter() {
		Zadd<String, String, Map<String, Object>> writer = new Zadd<>();
		writer.setScoreConverter(numberFieldExtractor(Double.class, scoreField, scoreDefault));
		return writer;
	}

}
