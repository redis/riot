package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.batch.redisearch.writer.AbstractLettuSearchMapWriter;
import com.redislabs.riot.batch.redisearch.writer.AbstractSearchMapWriter;
import com.redislabs.riot.batch.redisearch.writer.FtaddMapWriter;
import com.redislabs.riot.batch.redisearch.writer.FtaddPayloadMapWriter;
import com.redislabs.riot.batch.redisearch.writer.SugaddMapWriter;
import com.redislabs.riot.batch.redisearch.writer.SugaddPayloadMapWriter;
import com.redislabs.riot.cli.RediSearchCommand;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class RediSearchCommandOptions {

	@Option(names = { "-r",
			"--ft-command" }, description = "RediSearch command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RediSearchCommand command = RediSearchCommand.add;
	@Option(names = { "-i", "--index" }, description = "Name of the RediSearch index", paramLabel = "<name>")
	private String index;
	@Option(names = "--nosave", description = "Do not save docs, only index")
	private boolean noSave;
	@Option(names = "--replace", description = "UPSERT-style insertion")
	private boolean replace;
	@Option(names = "--partial", description = "Partial update (only applicable with replace)")
	private boolean partial;
	@Option(names = "--language", description = "Stemmer to use for indexing: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
	private Language language;
	@Option(names = "--if-condition", description = "Boolean expression for conditional update", paramLabel = "<exp>")
	private String ifCondition;
	@Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
	private String payloadField;
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--default-score", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double defaultScore = 1d;
	@Option(names = "--suggest", description = "Field containing the suggestion", paramLabel = "<field>")
	private String suggestField;
	@Option(names = "--suggest-increment", description = "Use increment to set value")
	private boolean increment;

	private AbstractSearchMapWriter searchWriter() {
		if (payloadField == null) {
			return new FtaddMapWriter();
		}
		FtaddPayloadMapWriter writer = new FtaddPayloadMapWriter();
		writer.setPayloadField(payloadField);
		return writer;
	}

	private SugaddMapWriter suggestWriter() {
		if (payloadField == null) {
			return new SugaddMapWriter();
		}
		SugaddPayloadMapWriter writer = new SugaddPayloadMapWriter();
		writer.setPayloadField(payloadField);
		return writer;
	}

	public AbstractLettuSearchMapWriter writer() {
		switch (command) {
		case sugadd:
			SugaddMapWriter suggestWriter = suggestWriter();
			suggestWriter.setIndex(index);
			suggestWriter.setScoreField(scoreField);
			suggestWriter.setDefaultScore(defaultScore);
			suggestWriter.setField(suggestField);
			suggestWriter.setIncrement(increment);
			return suggestWriter;
		default:
			AbstractSearchMapWriter addWriter = searchWriter();
			addWriter.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
					.replace(replace).replacePartial(partial).build());
			addWriter.setDefaultScore(defaultScore);
			addWriter.setScoreField(scoreField);
			addWriter.setIndex(index);
			return addWriter;
		}
	}

	public boolean isSet() {
		return index != null;
	}

}