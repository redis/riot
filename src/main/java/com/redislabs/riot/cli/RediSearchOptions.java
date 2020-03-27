package com.redislabs.riot.cli;

import java.util.List;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.map.FtAdd;
import com.redislabs.riot.redis.writer.map.FtAdd.FtAddBuilder;
import com.redislabs.riot.redis.writer.map.FtAddPayload;
import com.redislabs.riot.redis.writer.map.FtAddPayload.FtAddPayloadBuilder;
import com.redislabs.riot.redis.writer.map.FtAggregate;
import com.redislabs.riot.redis.writer.map.FtAggregate.FtAggregateBuilder;
import com.redislabs.riot.redis.writer.map.FtSearch;
import com.redislabs.riot.redis.writer.map.FtSearch.FtSearchBuilder;
import com.redislabs.riot.redis.writer.map.FtSugadd;
import com.redislabs.riot.redis.writer.map.FtSugadd.FtSugaddBuilder;
import com.redislabs.riot.redis.writer.map.FtSugaddPayload;
import com.redislabs.riot.redis.writer.map.FtSugaddPayload.FtSugaddPayloadBuilder;

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
	@Option(names = "--query", description = "RediSearch query", paramLabel = "<string>")
	private String query;
	@Option(names = "--options", arity = "1..*", description = "Search/aggregate options", paramLabel = "<string>")
	private List<String> options;

	public boolean hasPayload() {
		return payload != null;
	}

	public FtSearchBuilder search() {
		return FtSearch.builder().index(index).query(query).options(options);
	}

	public FtAggregateBuilder aggregate() {
		return FtAggregate.builder().index(index).query(query).options(options);
	}

	public FtAddBuilder ftAdd() {
		return FtAdd.builder().index(index).options(addOptions());
	}

	public FtAddPayloadBuilder ftAddPayload() {
		return FtAddPayload.builder().index(index).options(addOptions()).payload(payload);
	}

	private AddOptions addOptions() {
		return AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave).replace(replace)
				.replacePartial(partial).build();
	}

	public FtSugaddBuilder sugadd() {
		return FtSugadd.builder().field(suggest).increment(increment);
	}

	public FtSugaddPayloadBuilder sugaddPayload() {
		return FtSugaddPayload.builder().field(suggest).increment(increment).payload(payload);
	}

}
