package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.recharge.RechargeConfiguration.AbstractRedisWriterConfiguration;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HMSetWriter extends AbstractPipelineRedisWriter {

	public HMSetWriter(AbstractRedisWriterConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record) {
		commands.hmset(key, convert(record));
	}

}