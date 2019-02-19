package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.suggest.SuggestAddOptions;
import com.redislabs.recharge.redis.SuggestConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class SuggestionWriter extends AbstractPipelineRedisWriter<SuggestConfiguration> {

	private SuggestAddOptions options;

	public SuggestionWriter(SuggestConfiguration config) {
		super(config);
		options = SuggestAddOptions.builder().increment(config.isIncrement()).build();
	}

	@Override
	protected RedisFuture<Long> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		String string = converter.convert(record.get(config.getField()), String.class);
		double score = getScore(record);
		return commands.sugadd(key, string, score, options);
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
