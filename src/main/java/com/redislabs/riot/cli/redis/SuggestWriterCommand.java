package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.search.AbstractLettuSearchItemWriter;
import com.redislabs.riot.redis.writer.search.JedisSuggestWriter;
import com.redislabs.riot.redis.writer.search.SuggestPayloadWriter;
import com.redislabs.riot.redis.writer.search.SuggestWriter;

import io.redisearch.client.Client;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;

@Command(name = "suggest", description = "RediSearch suggestion index")
public class SuggestWriterCommand extends AbstractRediSearchWriterCommand {

	@Option(names = "--field", description = "Name of the field containing the suggestion", paramLabel = "<field>")
	private String field;
	@Option(names = "--increment", description = "Use increment to set value")
	private boolean increment;

	@Override
	protected AbstractLettuSearchItemWriter rediSearchWriter() {
		SuggestWriter suggestItemWriter = suggestItemWriter();
		suggestItemWriter.setField(field);
		suggestItemWriter.setIncrement(increment);
		return suggestItemWriter;
	}

	private SuggestWriter suggestItemWriter() {
		if (payloadField == null) {
			return new SuggestWriter();
		}
		return new SuggestPayloadWriter(payloadField);
	}

	private JedisSuggestWriter jedisSuggestWriter(JedisPool pool) {
		JedisSuggestWriter writer = new JedisSuggestWriter(new Client(index, pool), redisConverter());
		writer.setDefaultScore((float) defaultScore);
		writer.setScoreField(scoreField);
		writer.setPayloadField(payloadField);
		writer.setIncrement(increment);
		writer.setField(field);
		return writer;
	}

}
