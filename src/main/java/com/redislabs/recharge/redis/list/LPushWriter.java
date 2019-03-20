package com.redislabs.recharge.redis.list;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.CollectionRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class LPushWriter extends CollectionRedisWriter<ListConfiguration> {

	public LPushWriter(ListConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands) {
		return commands.lpush(key, member);
	}

}
