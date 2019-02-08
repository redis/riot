package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.RechargeConfiguration.SetConfiguration;

@SuppressWarnings("rawtypes")
public class SAddWriter extends AbstractPipelineRedisWriter<SetConfiguration> {

	public SAddWriter(SetConfiguration config) {
		super(config);
	}
	
	@Override
	protected void write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		commands.sadd(key, getValues(record, config.getFields()));
	}

}
