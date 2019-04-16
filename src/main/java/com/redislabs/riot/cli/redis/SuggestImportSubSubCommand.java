package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.search.SuggestWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "suggest", description = "Suggestion index")
public class SuggestImportSubSubCommand extends AbstractRedisImportSubSubCommand {

	@Option(names = "--index", description = "Name of the suggestion index", order = 3)
	private String index;
	@Option(names = "--score-field", description = "Name of the field to use for scores.", order = 5)
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).", order = 5)
	private Double defaultScore = 1d;
	@Option(names = "--increment", description = "Use increment to set value", order = 3)
	private boolean increment;
	@Option(names = "--suggest-field", description = "Name of the field containing the suggestion", order = 3)
	private String suggestField;
	@Option(names = "--payload-field", description = "Name of the field containing the payload", order = 3)
	private String payloadField;

	@Override
	protected SuggestWriter createWriter() {
		SuggestWriter writer = new SuggestWriter();
		writer.setDefaultScore(defaultScore);
		writer.setField(suggestField);
		writer.setIncrement(increment);
		writer.setIndex(index);
		writer.setPayloadField(payloadField);
		writer.setScoreField(scoreField);
		return writer;
	}

	@Override
	public String getTargetDescription() {
		return "suggestion index " + index;
	}

}
