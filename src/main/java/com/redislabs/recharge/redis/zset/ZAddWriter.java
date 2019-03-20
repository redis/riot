package com.redislabs.recharge.redis.zset;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.CollectionRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class ZAddWriter extends CollectionRedisWriter<ZSetConfiguration> {

	public ZAddWriter(ZSetConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		return commands.zadd(key, score, member);
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
