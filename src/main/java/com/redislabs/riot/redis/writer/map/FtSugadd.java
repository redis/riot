package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class FtSugadd extends AbstractSearchMapCommandWriter {

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
		return write(commands, redis, key, item, string, getScore(item), increment);
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

	public static class FtSugaddPayload extends FtSugadd {

		@Setter
		private String payload;

		@Override
		protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item,
				String string, double score, boolean increment) {
			return commands.sugadd(redis, key, string, score, increment, convert(item.remove(payload), String.class));
		}

	}

}
