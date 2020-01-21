package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class FtSugadd extends AbstractRediSearchWriter {

	@Setter
	private String field;
	@Setter
	private boolean increment;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		String string = convert(item.get(field), String.class);
		if (string == null) {
			return null;
		}
		return write(commands, redis, key, item, string, score(item), increment);
	}

	@SuppressWarnings("unused")
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item, String string,
			double score, boolean increment) {
		return commands.sugadd(redis, key, string, score, increment);
	}

	@Override
	public String toString() {
		return String.format("RediSearch suggestion index");
	}

}
