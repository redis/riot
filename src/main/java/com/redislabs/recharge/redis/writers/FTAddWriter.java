package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.recharge.redis.SearchConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class FTAddWriter extends AbstractPipelineRedisWriter<SearchConfiguration> {

	private AddOptions options;

	public FTAddWriter(SearchConfiguration config) {
		super(config);
		options = AddOptions.builder().noSave(config.isNoSave()).replace(config.isReplace())
				.replacePartial(config.isReplacePartial()).build();
	}

	@Override
	protected RedisFuture<?> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		convert(record);
		return commands.add(config.getKeyspace(), key, score, record, options);
	}

	private double getScore(Map<String, Object> record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
