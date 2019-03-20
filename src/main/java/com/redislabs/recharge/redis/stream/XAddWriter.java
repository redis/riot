package com.redislabs.recharge.redis.stream;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.SingleRedisWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XAddWriter extends SingleRedisWriter<StreamConfiguration> {

	public XAddWriter(StreamConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> writeSingle(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(config.isApproximateTrimming());
		if (config.getId() != null) {
			args.id(converter.convert(record.getOrDefault(config.getId(), config.getId()), String.class));
		}
		if (config.getMaxlen() != null) {
			args.maxlen(config.getMaxlen());
		}
		convert(record);
		return commands.xadd(key, args, record);
	}

}