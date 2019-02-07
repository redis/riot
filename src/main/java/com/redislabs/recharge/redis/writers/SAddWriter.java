package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.recharge.RechargeConfiguration.SetConfiguration;

@SuppressWarnings("rawtypes")
public class SAddWriter extends AbstractPipelineRedisWriter<SetConfiguration> {

	public SAddWriter(SetConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record) {
		commands.sadd(key, getValues(record, config.getFields()));
	}

}
