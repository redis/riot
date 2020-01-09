package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class FtSugaddPayload<R> extends FtSugadd<R> {

	@Setter
	private String payload;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item, String string,
			double score, boolean increment) {
		return commands.sugadd(redis, key, string, score, increment, convert(item.remove(payload), String.class));
	}

}
