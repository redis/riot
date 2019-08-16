package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redisearch.AbstractLettuSearchItemWriter;
import com.redislabs.riot.redisearch.AbstractSearchItemWriter;
import com.redislabs.riot.redisearch.SearchItemWriter;
import com.redislabs.riot.redisearch.SearchPayloadItemWriter;
import com.redislabs.riot.redisearch.SuggestItemWriter;
import com.redislabs.riot.redisearch.SuggestPayloadItemWriter;

import picocli.CommandLine.Option;

public class RediSearchWriterOptions {

	@Option(names = "--index", description = "Name of the RediSearch index")
	private String index;
	@Option(names = "--ids", arity = "1..*", description = "Fields to use to build document ids", paramLabel = "<names>")
	private String[] ids = new String[0];
	@Option(names = "--id-separator", description = "Document id separator (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String idSeparator = ":";
	@Option(names = "--id-prefix", description = "Document id prefix", paramLabel = "<string>")
	private String idPrefix;
	@Option(names = "--nosave", description = "Do not save the actual document in the database and only index it")
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists")
	private boolean replace;
	@Option(names = "--partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing")
	private boolean partial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}")
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
	private String field;
	@Option(names = "--suggest-increment", description = "Use increment to set value")
	private boolean increment;

	public String getIndex() {
		return index;
	}

	public AbstractLettuSearchItemWriter writer() {
		AbstractLettuSearchItemWriter itemWriter = itemWriter();
		itemWriter.setConverter(new RedisConverter(idSeparator, idPrefix, ids));
		return itemWriter;
	}

	private AbstractLettuSearchItemWriter itemWriter() {
		if (field == null) {
			return searchWriter();
		}
		return suggestWriter();
	}

	private AbstractSearchItemWriter searchWriter() {
		if (payloadField == null) {
			return configure(new SearchItemWriter());
		}
		SearchPayloadItemWriter writer = new SearchPayloadItemWriter();
		writer.setPayloadField(payloadField);
		return configure(writer);
	}

	private SuggestItemWriter suggestWriter() {
		if (payloadField == null) {
			return configure(new SuggestItemWriter());
		}
		SuggestPayloadItemWriter writer = new SuggestPayloadItemWriter();
		writer.setPayloadField(payloadField);
		return configure(writer);
	}

	private SuggestItemWriter configure(SuggestItemWriter writer) {
		writer.setIndex(index);
		writer.setScoreField(scoreField);
		writer.setDefaultScore(defaultScore);
		writer.setField(field);
		writer.setIncrement(increment);
		return writer;
	}

	private AbstractSearchItemWriter configure(AbstractSearchItemWriter writer) {
		writer.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(partial).build());
		writer.setDefaultScore(defaultScore);
		writer.setScoreField(scoreField);
		writer.setIndex(index);
		return writer;
	}

	public boolean isSet() {
		return index != null;
	}

}