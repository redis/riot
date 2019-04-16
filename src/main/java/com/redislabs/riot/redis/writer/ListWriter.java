package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class ListWriter extends AbstractRedisCollectionWriter {

	public enum PushDirection {
		Left, Right
	}

	private PushDirection pushDirection;

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		if (pushDirection == PushDirection.Right) {
			return commands.rpush(key, member);
		}
		return commands.lpush(key, member);
	}

}