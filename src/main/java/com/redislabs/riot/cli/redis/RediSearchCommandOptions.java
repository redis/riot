package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redisearch.AbstractLettuSearchItemWriter;
import com.redislabs.riot.redisearch.AbstractSearchItemWriter;
import com.redislabs.riot.redisearch.FtaddItemWriter;
import com.redislabs.riot.redisearch.FtaddPayloadItemWriter;
import com.redislabs.riot.redisearch.SugaddItemWriter;
import com.redislabs.riot.redisearch.SugaddPayloadItemWriter;

import picocli.CommandLine.Option;

public class RediSearchCommandOptions {

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

	private AbstractSearchItemWriter searchWriter() {
		if (payloadField == null) {
			return new FtaddItemWriter();
		}
		FtaddPayloadItemWriter writer = new FtaddPayloadItemWriter();
		writer.setPayloadField(payloadField);
		return writer;
	}

	private SugaddItemWriter suggestWriter() {
		if (payloadField == null) {
			return new SugaddItemWriter();
		}
		SugaddPayloadItemWriter writer = new SugaddPayloadItemWriter();
		writer.setPayloadField(payloadField);
		return writer;
	}

	public AbstractLettuSearchItemWriter addWriter() {
		AbstractSearchItemWriter writer = searchWriter();
		writer.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(partial).build());
		writer.setDefaultScore(defaultScore);
		writer.setScoreField(scoreField);
		writer.setIndex(index);
		return writer;
	}

	public AbstractLettuSearchItemWriter sugaddWriter() {
		SugaddItemWriter writer = suggestWriter();
		writer.setIndex(index);
		writer.setScoreField(scoreField);
		writer.setDefaultScore(defaultScore);
		writer.setField(suggestField);
		writer.setIncrement(increment);
		return writer;
	}

}