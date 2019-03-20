package com.redislabs.recharge.redis.set;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.CollectionRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class SAddWriter extends CollectionRedisWriter<SetConfiguration> {

	public SAddWriter(SetConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands) {
		return commands.sadd(key, member);
	}

}