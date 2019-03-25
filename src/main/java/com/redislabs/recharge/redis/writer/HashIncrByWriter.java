package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class HashIncrByWriter extends AbstractSingleRedisWriter {

	private String field;
	private String incrementField;
	private double defaultIncrement;

	@Override
	protected RedisFuture<?> writeSingle(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		Object increment = record.getOrDefault(incrementField, defaultIncrement);
		Long amount = converter.convert(increment, Long.class);
		return commands.hincrby(key, field, amount);
	}

}