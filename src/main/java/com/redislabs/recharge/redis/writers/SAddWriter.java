package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.SetConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class SAddWriter extends AbstractPipelineRedisWriter<SetConfiguration> {

	public SAddWriter(SetConfiguration config) {
		super(config);
	}
	
	@Override
	protected RedisFuture<Long> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		return commands.sadd(key, getValues(record, config.getFields()));
	}

}
