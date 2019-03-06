package com.redislabs.recharge.redis.suggest;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.suggest.SuggestAddOptions;
import com.redislabs.recharge.redis.SingleRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class SuggestWriter extends SingleRedisWriter<SuggestConfiguration> {

	private SuggestAddOptions options;

	public SuggestWriter(SuggestConfiguration config,
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		super(config, pool);
		options = SuggestAddOptions.builder().increment(config.isIncrement()).build();
	}

	@Override
	protected RedisFuture<?> writeSingle(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		String string = converter.convert(record.get(config.getField()), String.class);
		double score = getScore(record);
		if (string == null) {
			return null;
		}
		return commands.sugadd(key, string, score, options);
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
