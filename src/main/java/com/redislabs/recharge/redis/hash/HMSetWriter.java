package com.redislabs.recharge.redis.hash;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.SingleRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HMSetWriter extends SingleRedisWriter<HashConfiguration> {

	public HMSetWriter(HashConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> writeSingle(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		convert(record);
		return commands.hmset(key, record);
	}

}