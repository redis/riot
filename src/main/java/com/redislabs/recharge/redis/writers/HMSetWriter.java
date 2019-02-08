package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.RechargeConfiguration.HashConfiguration;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HMSetWriter extends AbstractPipelineRedisWriter<HashConfiguration> {

	public HMSetWriter(HashConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		commands.hmset(key, convert(record));
	}

}