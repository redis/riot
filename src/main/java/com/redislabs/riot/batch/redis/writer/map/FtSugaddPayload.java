package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class FtSugaddPayload<R> extends FtSugadd<R> {

	@Setter
	private String payload;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item, String string,
			double score, boolean increment) {
		return commands.sugadd(redis, key, string, score, increment, convert(item.remove(payload), String.class));
	}

}
