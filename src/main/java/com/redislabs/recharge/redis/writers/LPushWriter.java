package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.ListConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class LPushWriter extends AbstractPipelineRedisWriter<ListConfiguration> {

	public LPushWriter(ListConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<Long> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		return commands.lpush(key, getValues(record, config.getFields()));
	}

}
