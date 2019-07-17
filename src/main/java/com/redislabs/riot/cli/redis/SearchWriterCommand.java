package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.search.AbstractLettuSearchItemWriter;
import com.redislabs.riot.redis.writer.search.AbstractSearchWriter;
import com.redislabs.riot.redis.writer.search.JedisSearchWriter;
import com.redislabs.riot.redis.writer.search.SearchPayloadWriter;
import com.redislabs.riot.redis.writer.search.SearchWriter;

import io.redisearch.client.AddOptions.ReplacementPolicy;
import io.redisearch.client.Client;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;

@Command(name = "search", description = "RediSearch search index")
public class SearchWriterCommand extends AbstractRediSearchWriterCommand {

	@Option(names = "--nosave", description = "Do not save the actual document in the database and only index it")
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists")
	private boolean replace;
	@Option(names = "--partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing")
	private boolean partial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}")
	private Language language;
	@Option(names = "--if", description = "Applicable only in conjunction with REPLACE and optionally PARTIAL. Update the document only if a boolean expression applies to the document before the update", paramLabel = "<condition>")
	private String ifCondition;

	@Override
	protected AbstractLettuSearchItemWriter rediSearchWriter() {
		AbstractSearchWriter searchItemWriter = searchItemWriter();
		searchItemWriter.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
				.replace(replace).replacePartial(partial).build());
		return searchItemWriter;
	}

	private AbstractSearchWriter searchItemWriter() {
		if (payloadField == null) {
			return new SearchWriter();
		}
		return new SearchPayloadWriter(payloadField);
	}

	@SuppressWarnings("unused")
	private JedisSearchWriter jedisSearchWriter(JedisPool pool) {
		io.redisearch.client.AddOptions options = new io.redisearch.client.AddOptions();
		if (language != null) {
			options.setLanguage(language.name());
		}
		if (noSave) {
			options.setNosave();
		}
		options.setReplacementPolicy(replacementPolicy());
		JedisSearchWriter writer = new JedisSearchWriter(new Client(index, pool), redisConverter(), options);
		writer.setDefaultScore((float) defaultScore);
		writer.setScoreField(scoreField);
		writer.setPayloadField(payloadField);
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
