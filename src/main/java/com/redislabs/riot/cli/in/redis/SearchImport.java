package com.redislabs.riot.cli.in.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.search.JedisSearchWriter;
import com.redislabs.riot.redis.writer.search.SearchAddWriter;

import io.redisearch.client.AddOptions.ReplacementPolicy;
import io.redisearch.client.Client;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;

@Command(name = "search", description = "Search index")
public class SearchImport extends AbstractRediSearchImport {

	@Option(names = "--nosave", description = "Do not save the actual document in the database and only index it")
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists")
	private boolean replace;
	@Option(names = "--partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing.")
	private boolean partial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}")
	private Language language;
	@Option(names = "--if", description = "Applicable only in conjunction with REPLACE and optionally PARTIAL. Update the document only if a boolean expression applies to the document before the update.")
	private String ifCondition;
	@Option(names = "--score", description = "Name of the field to use for scores.")
	private String score;
	@Option(names = "--default-score", description = "Default score to use when score field is not present. (default: ${DEFAULT-VALUE}).")
	private double defaultScore = 1d;
	@Option(names = "--payload", description = "Name of the field containing the payload")
	private String payload;

	@Override
	protected SearchAddWriter rediSearchItemWriter() {
		SearchAddWriter writer = new SearchAddWriter();
		writer.setDefaultScore(defaultScore);
		writer.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(partial).build());
		writer.setPayloadField(payload);
		writer.setScoreField(score);
		return writer;
	}

	@Override
	protected ItemWriter<Map<String, Object>> jedisSearchWriter() {
		JedisSearchWriter writer = new JedisSearchWriter();
		JedisPool pool = getRoot().jedisPool();
		writer.setClient(new Client(getIndex(), pool));
		writer.setConverter(redisConverter());
		writer.setDefaultScore((float) defaultScore);
		writer.setScoreField(score);
		writer.setPayloadField(payload);
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
			if (partial) {
				return ReplacementPolicy.PARTIAL;
			}
			return ReplacementPolicy.FULL;
		}
		return ReplacementPolicy.NONE;
	}

}
