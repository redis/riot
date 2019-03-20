package com.redislabs.recharge.redis.hash;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.SingleRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class HIncrByWriter extends SingleRedisWriter<HashConfiguration> {

	public HIncrByWriter(HashConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> writeSingle(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		Long amount = converter.convert(record.get(config.getIncrby().getSourceField()), Long.class);
		String field = config.getIncrby().getTargetField();
		return commands.hincrby(key, field, amount);
	}

}