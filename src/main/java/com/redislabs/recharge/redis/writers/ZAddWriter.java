package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.ZSetConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class ZAddWriter extends AbstractPipelineRedisWriter<ZSetConfiguration> {

	public ZAddWriter(ZSetConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<Long> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		String member = getValues(record, config.getFields());
		return commands.zadd(key, score, member);
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
