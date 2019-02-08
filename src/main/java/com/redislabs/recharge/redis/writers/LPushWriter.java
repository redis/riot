package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.RechargeConfiguration.ListConfiguration;

@SuppressWarnings("rawtypes")
public class LPushWriter extends AbstractPipelineRedisWriter<ListConfiguration> {

	public LPushWriter(ListConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		commands.lpush(key, getValues(record, config.getFields()));
	}

}
