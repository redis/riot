package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zadd")
public class ZaddCommand extends AbstractCollectionCommand {

	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault = 1;

	@Override
	protected Zadd<String, String, Map<String, Object>> collectionWriter() {
		Zadd<String, String, Map<String, Object>> writer = new Zadd<>();
		writer.setScoreConverter(numberFieldExtractor(Double.class, scoreField, scoreDefault));
		return writer;
	}

}
