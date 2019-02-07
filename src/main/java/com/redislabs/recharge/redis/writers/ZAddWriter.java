package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.recharge.RechargeConfiguration.ZSetConfiguration;

@SuppressWarnings("rawtypes")
public class ZAddWriter extends AbstractPipelineRedisWriter<ZSetConfiguration> {

	public ZAddWriter(ZSetConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record) {
		commands.zadd(key, getScore(record), getValues(record, config.getFields()));
	}

	private double getScore(Map record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

}
