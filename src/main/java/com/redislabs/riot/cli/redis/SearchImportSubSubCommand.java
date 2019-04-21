package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.search.JedisSearchWriter;
import com.redislabs.riot.redis.writer.search.SearchAddWriter;

import io.redisearch.client.AddOptions.ReplacementPolicy;
import io.redisearch.client.Client;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "search", description = "Search index")
public class SearchImportSubSubCommand extends AbstractRediSearchImportSubSubCommand {

	@Option(names = "--no-save", description = "Do not save the actual document in the database and only index it")
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists")
	private boolean replace;
	@Option(names = "--replace-partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing.")
	private boolean replacePartial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}")
	private Language language;
	@Option(names = "--if-condition", description = "Applicable only in conjunction with REPLACE and optionally PARTIAL. Update the document only if a boolean expression applies to the document before the update.")
	private String ifCondition;
	@Option(names = "--score-field", description = "Name of the field to use for scores.")
	private String scoreField;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).")
	private Double defaultScore = 1d;
	@Option(names = "--payload-field", description = "Name of the field containing the payload")
	private String payloadField;

	@Override
	protected SearchAddWriter rediSearchItemWriter() {
		SearchAddWriter writer = new SearchAddWriter();
		writer.setDefaultScore(defaultScore);
		writer.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(replacePartial).build());
		writer.setPayloadField(payloadField);
		writer.setScoreField(scoreField);
		return writer;
	}

	@Override
	protected ItemStreamWriter<Map<String, Object>> jedisWriter() {
		JedisSearchWriter writer = new JedisSearchWriter();
		writer.setClient(new Client(getIndex(), parent.getParent().redisConnectionBuilder().buildJedisPool()));
		writer.setConverter(redisConverter());
		if (defaultScore != null) {
			writer.setDefaultScore(defaultScore.floatValue());
		}
		writer.setScoreField(scoreField);
		writer.setPayloadField(payloadField);
		io.redisearch.client.AddOptions options = new io.redisearch.client.AddOptions();
		if (language != null) {
			options.setLanguage(language.name());
		}
		if (noSave) {
			options.setNosave();
		}
		options.setReplacementPolicy(replacementPolicy());
		writer.setOptions(options);
		return writer;
	}

	private ReplacementPolicy replacementPolicy() {
		if (replace) {
			if (replacePartial) {
				return ReplacementPolicy.PARTIAL;
			}
			return ReplacementPolicy.FULL;
		}
		return ReplacementPolicy.NONE;
	}

	@Override
	protected String getDataStructure() {
		return "search index";
	}

}
