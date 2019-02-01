package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.suggest.SuggestAddOptions;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.SuggestConfiguration;

public class SuggestionWriter extends AbstractPipelineRedisWriter {

	private SuggestConfiguration suggest;

	public SuggestionWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer);
		this.suggest = writer.getSuggest();
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		String string = convert(record.get(suggest.getField()), String.class);
		double score = suggest.getScore() == null ? suggest.getDefaultScore()
				: convert(record.get(suggest.getScore()), Double.class);
		commands.sugadd(key, string, score, SuggestAddOptions.builder().increment(suggest.isIncrement()).build());
	}

}
