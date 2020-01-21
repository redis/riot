package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class FtSugaddPayload extends FtSugadd {

	@Setter
	private String payload;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item, String string,
			double score, boolean increment) {
		return commands.sugadd(redis, key, string, score, increment, convert(item.remove(payload), String.class));
	}

}
