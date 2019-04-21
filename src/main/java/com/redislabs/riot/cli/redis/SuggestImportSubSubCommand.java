package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.search.AbstractRediSearchItemWriter;
import com.redislabs.riot.redis.writer.search.SuggestWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "suggest", description = "Suggestion index")
public class SuggestImportSubSubCommand extends AbstractRediSearchImportSubSubCommand {

	@Option(names = "--score-field", description = "Name of the field to use for scores.")
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).")
	private Double defaultScore = 1d;
	@Option(names = "--increment", description = "Use increment to set value")
	private boolean increment;
	@Option(names = "--suggest-field", description = "Name of the field containing the suggestion")
	private String suggestField;
	@Option(names = "--payload-field", description = "Name of the field containing the payload")
	private String payloadField;

	@Override
	protected AbstractRediSearchItemWriter rediSearchItemWriter() {
		SuggestWriter writer = new SuggestWriter();
		writer.setDefaultScore(defaultScore);
		writer.setField(suggestField);
		writer.setIncrement(increment);
		writer.setPayloadField(payloadField);
		writer.setScoreField(scoreField);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "suggestion index";
	}

}
