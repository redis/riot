package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.RediSearchCommandWriter;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractFtSugadd extends AbstractKeyMapCommandWriter
		implements RediSearchCommandWriter<Map<String, Object>> {

	private @Setter String field;
	private @Setter boolean increment;
	private @Setter String score;
	private @Setter double defaultScore = 1d;

	protected AbstractFtSugadd(KeyBuilder keyBuilder, boolean keepKeyFields, String field, String score,
			double defaultScore, boolean increment) {
		super(keyBuilder, keepKeyFields);
		this.field = field;
		this.score = score;
		this.defaultScore = defaultScore;
		this.increment = increment;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		String string = convert(item.get(field), String.class);
		if (string == null) {
			return null;
		}
		return write(commands, redis, key, item, string, convert(item.getOrDefault(score, defaultScore), Double.class),
				increment);
	}

	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item, String string,
			double score, boolean increment) {
		return commands.sugadd(redis, key, string, score, increment, payload(item));
	}

	protected abstract String payload(Map<String, Object> item);

}
