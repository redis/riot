package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.RedisType;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class ListWriter extends AbstractCollectionRedisWriter {

	private boolean right;

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		if (right) {
			return commands.rpush(key, member);
		}
		return commands.lpush(key, member);
	}

	@Override
	public RedisType getRedisType() {
		return RedisType.List;
	}

}
