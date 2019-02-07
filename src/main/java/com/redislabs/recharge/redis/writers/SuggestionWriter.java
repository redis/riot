package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.suggest.SuggestAddOptions;
import com.redislabs.recharge.RechargeConfiguration.SuggestConfiguration;

@SuppressWarnings("rawtypes")
public class SuggestionWriter extends AbstractPipelineRedisWriter<SuggestConfiguration> {

	public SuggestionWriter(SuggestConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record) {
		String string = converter.convert(record.get(config.getField()), String.class);
		commands.sugadd(key, string, getScore(record),
				SuggestAddOptions.builder().increment(config.isIncrement()).build());
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
