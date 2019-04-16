package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.cli.AbstractImportSubSubCommand;
import com.redislabs.riot.redis.writer.search.SearchAddWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "search", description = "Search index")
public class SearchImportSubSubCommand extends AbstractImportSubSubCommand {

	@Option(names = "--index", description = "Name of the search index", order = 3)
	private String index;
	@Option(arity = "1..*", names = "--keys", description = "Fields used to build the document id.", order = 3)
	private String[] keys;
	@Option(names = "--drop-index", description = "Drop index before writing", order = 6)
	private boolean dropIndex;
	@Option(names = "--keep-docs", description = "Keep documents when dropping index", order = 6)
	private boolean keepDocs;
	@Option(names = "--no-save", description = "Do not save the actual document in the database and only index it", order = 6)
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists", order = 6)
	private boolean replace;
	@Option(names = "--replace-partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing.", order = 6)
	private boolean replacePartial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}", order = 6)
	private Language language;
	@Option(names = "--if-condition", description = "Applicable only in conjunction with REPLACE and optionally PARTIAL. Update the document only if a boolean expression applies to the document before the update.", order = 6)
	private String ifCondition;
	@Option(names = "--score-field", description = "Name of the field to use for scores.", order = 5)
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).", order = 5)
	private Double defaultScore = 1d;
	@Option(names = "--payload-field", description = "Name of the field containing the payload", order = 3)
	private String payloadField;

	@Override
	protected SearchAddWriter redisWriter() {
		SearchAddWriter writer = new SearchAddWriter();
		writer.setDefaultScore(defaultScore);
		writer.setDrop(dropIndex);
		writer.setDropKeepDocs(keepDocs);
		writer.setIndex(index);
		writer.setKeys(keys);
		writer.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(replacePartial).build());
		writer.setPayloadField(payloadField);
		writer.setScoreField(scoreField);
		return writer;
	}

	@Override
	public String getTargetDescription() {
		return "search index \"" + index + "\"";
	}

}
