package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.RechargeConfiguration.HashConfiguration;

@SuppressWarnings("rawtypes")
public class HIncrByWriter extends AbstractPipelineRedisWriter<HashConfiguration> {

	public HIncrByWriter(HashConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		commands.hincrby(key, config.getIncrby().getTargetField(), getDelta(record));
	}

	private long getDelta(Map record) {
		return converter.convert(record.get(config.getIncrby().getSourceField()), Long.class);
	}

}