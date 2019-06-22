package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.LettuceAsyncWriter;
import com.redislabs.riot.redis.writer.search.AbstractLettuSearchItemWriter;
import com.redislabs.riot.redis.writer.search.AbstractSearchWriter;
import com.redislabs.riot.redis.writer.search.JedisSearchWriter;
import com.redislabs.riot.redis.writer.search.JedisSuggestWriter;
import com.redislabs.riot.redis.writer.search.SearchPayloadWriter;
import com.redislabs.riot.redis.writer.search.SearchWriter;
import com.redislabs.riot.redis.writer.search.SuggestPayloadWriter;
import com.redislabs.riot.redis.writer.search.SuggestWriter;

import io.redisearch.client.AddOptions.ReplacementPolicy;
import io.redisearch.client.Client;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;

@Command(name = "redisearch", description = "RediSearch index")
public class RediSearchWriterCommand extends AbstractRedisWriterCommand<RediSearchAsyncCommands<String, String>> {

	enum IndexType {
		Search, Suggest
	}

	@Option(names = "--index", required = true, description = "Name of the RediSearch index.", paramLabel = "<name>")
	private String index;
	@Option(names = "--type", description = "Index type.")
	private IndexType type = IndexType.Search;
	@Option(names = "--nosave", description = "Do not save the actual document in the database and only index it.")
	private boolean noSave;
	@Option(names = "--replace", description = "Do an UPSERT style insertion and delete an older version of the document if it exists.")
	private boolean replace;
	@Option(names = "--partial", description = "Only applicable with replace. If set, you do not have to specify all fields for reindexing.")
	private boolean partial;
	@Option(names = "--language", description = "Use a stemmer for the supplied language during indexing. Languages supported: ${COMPLETION-CANDIDATES}.")
	private Language language;
	@Option(names = "--if", description = "Applicable only in conjunction with REPLACE and optionally PARTIAL. Update the document only if a boolean expression applies to the document before the update.", paramLabel = "<condition>")
	private String ifCondition;
	@Option(names = "--default-score", description = "Default score to use when score field is not present.", paramLabel = "<score>")
	private double defaultScore = 1d;
	@Option(names = "--payload", description = "Name of the field containing the payload.", paramLabel = "<field>")
	private String payloadField;
	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--increment", description = "Use increment to set value.")
	private boolean increment;
	@Option(names = "--suggest", description = "Name of the field containing the suggestion.", paramLabel = "<field>")
	private String suggestField;

	@Override
	protected String getTargetDescription() {
		return "index \"" + index + "\"";
	}

	@Override
	protected LettuceAsyncWriter<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>> lettuceWriter(
			AbstractRedisItemWriter<RediSearchAsyncCommands<String, String>> itemWriter) {
		return new LettuceAsyncWriter<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
				redis.lettusearchPool(), itemWriter);
	}

	@Override
	protected AbstractRedisItemWriter<RediSearchAsyncCommands<String, String>> redisItemWriter() {
		AbstractLettuSearchItemWriter writer = rediSearchItemWriter();
		writer.setIndex(index);
		writer.setScoreField(scoreField);
		writer.setDefaultScore(defaultScore);
		return writer;
	}

	private AbstractLettuSearchItemWriter rediSearchItemWriter() {
		switch (type) {
		case Suggest:
			SuggestWriter suggestWriter = suggestItemWriter();
			suggestWriter.setField(suggestField);
			suggestWriter.setIncrement(increment);
			return suggestWriter;
		default:
			AbstractSearchWriter searchWriter = searchItemWriter();
			searchWriter.setOptions(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
					.replace(replace).replacePartial(partial).build());
			return searchWriter;
		}
	}

	private AbstractSearchWriter searchItemWriter() {
		if (payloadField == null) {
			return new SearchWriter();
		}
		SearchPayloadWriter writer = new SearchPayloadWriter();
		writer.setPayloadField(payloadField);
		return writer;
	}

	private SuggestWriter suggestItemWriter() {
		if (payloadField == null) {
			return new SuggestWriter();
		}
		SuggestPayloadWriter writer = new SuggestPayloadWriter();
		writer.setPayloadField(payloadField);
		return writer;
	}

	@Override
	protected ItemWriter<Map<String, Object>> jedisWriter(JedisPool pool,
			AbstractRedisItemWriter<RediSearchAsyncCommands<String, String>> itemWriter) {
		switch (type) {
		case Suggest:
			return jedisSuggestWriter(pool);
		default:
			return jedisSearchWriter(pool);
		}
	}

	private JedisSuggestWriter jedisSuggestWriter(JedisPool pool) {
		JedisSuggestWriter writer = new JedisSuggestWriter(new Client(index, pool), redisConverter());
		writer.setDefaultScore((float) defaultScore);
		writer.setScoreField(scoreField);
		writer.setPayloadField(payloadField);
		writer.setIncrement(increment);
		writer.setField(suggestField);
		return writer;
	}

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
