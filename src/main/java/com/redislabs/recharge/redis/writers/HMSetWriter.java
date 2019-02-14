package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.RechargeConfiguration.HashConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HMSetWriter extends AbstractPipelineRedisWriter<HashConfiguration> {

	public HMSetWriter(HashConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<String> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		convert(record);
		return commands.hmset(key, record);
	}

}