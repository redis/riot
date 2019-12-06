package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class FtSugadd<R> extends AbstractRediSearchWriter<R> {

	@Setter
	private String field;
	@Setter
	private boolean increment;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		String string = convert(item.get(field), String.class);
		if (string == null) {
			return null;
		}
		return write(commands, redis, key, item, string, score(item), increment);
	}

	@SuppressWarnings("unused")
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item, String string,
			double score, boolean increment) {
		return commands.sugadd(redis, key, string, score, increment);
	}

	@Override
	public String toString() {
		return String.format("RediSearch suggestion index");
	}

}
