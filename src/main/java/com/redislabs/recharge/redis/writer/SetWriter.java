package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.RedisType;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class SetWriter extends AbstractCollectionRedisWriter {

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		return commands.sadd(key, member);
	}

	@Override
	public RedisType getRedisType() {
		return RedisType.Set;
	}

}
