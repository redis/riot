package com.redislabs.riot.cli;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.map.FtAdd;
import com.redislabs.riot.redis.writer.map.FtSugadd;
import com.redislabs.riot.redis.writer.map.FtAdd.FtAddPayload;
import com.redislabs.riot.redis.writer.map.FtSugadd.FtSugaddPayload;

import picocli.CommandLine.Option;

public class RediSearchOptions {
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
	private String payload;
	@Option(names = "--suggest", description = "Field containing the suggestion", paramLabel = "<field>")
	private String suggest;
	@Option(names = "--increment", description = "Use increment to set value")
	private boolean increment;

	public FtAdd add() {
		FtAdd add = payload == null ? new FtAdd() : new FtAddPayload().payload(payload);
		return add.options(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(partial).build()).index(index);
	}

	public FtSugadd sugadd() {
		FtSugadd sugadd = payload == null ? new FtSugadd() : new FtSugaddPayload().payload(payload);
		return sugadd.field(suggest).increment(increment);
	}

}
