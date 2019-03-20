package com.redislabs.recharge.redis.suggest;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.suggest.SuggestAddOptions;
import com.redislabs.recharge.redisearch.RediSearchCommandWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class SuggestWriter extends RediSearchCommandWriter<SuggestConfiguration> {

	private SuggestAddOptions options;

	public SuggestWriter(SuggestConfiguration config) {
		super(config);
		options = SuggestAddOptions.builder().increment(config.isIncrement()).build();
	}

	@Override
	protected RedisFuture<?> write(Map record, RediSearchAsyncCommands<String, String> commands) {
		String string = converter.convert(record.get(config.getField()), String.class);
		double score = getScore(record);
		if (string == null) {
			return null;
		}
		return commands.sugadd(config.getIndex(), string, score, options);
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
