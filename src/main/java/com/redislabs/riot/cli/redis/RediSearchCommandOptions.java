package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.cli.RediSearchCommand;
import com.redislabs.riot.redisearch.AbstractLettuSearchMapWriter;
import com.redislabs.riot.redisearch.AbstractSearchMapWriter;
import com.redislabs.riot.redisearch.FtaddMapWriter;
import com.redislabs.riot.redisearch.FtaddPayloadMapWriter;
import com.redislabs.riot.redisearch.SugaddMapWriter;
import com.redislabs.riot.redisearch.SugaddPayloadMapWriter;

import picocli.CommandLine.Option;

public class RediSearchCommandOptions {

	@Option(names = { "-r",
			"--redisearch-command" }, description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RediSearchCommand command = RediSearchCommand.add;
	@Option(names = { "-i", "--index" }, description = "Name of the RediSearch index", paramLabel = "<name>")
	private String index;
	@Option(names = "--nosave", description = "Do not save the actual document in the database and only index it")
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists")
	private boolean replace;
	@Option(names = "--partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing")
	private boolean partial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
	private Language language;
	@Option(names = "--if-condition", description = "Update the document only if a boolean expression applies to the document before the update. Applicable only in conjunction with REPLACE and optionally PARTIAL.", paramLabel = "<condition>")
	private String ifCondition;
	@Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
	private String payloadField;
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--default-score", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<float>")
	private double defaultScore = 1d;
	@Option(names = "--suggest-field", description = "Name of the field containing the suggestion", paramLabel = "<field>")
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