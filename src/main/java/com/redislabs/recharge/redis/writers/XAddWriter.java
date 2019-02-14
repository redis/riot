package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.StreamConfiguration;

import io.lettuce.core.XAddArgs;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XAddWriter extends AbstractSyncRedisWriter<StreamConfiguration> {

	public XAddWriter(StreamConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record, RediSearchCommands<String, String> commands) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(config.isApproximateTrimming());
		if (config.getId() != null) {
			args.id(converter.convert(record.getOrDefault(config.getId(), config.getId()), String.class));
		}
		if (config.getMaxlen() != null) {
			args.maxlen(config.getMaxlen());
		}
		convert(record);
		commands.xadd(key, args, record);
	}

}