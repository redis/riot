package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings("unchecked")
@Accessors(fluent = true)
public class FtAdd<R> extends AbstractKeyMapRedisWriter<R> {

	@Setter
	private String index;
	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;
	@Setter
	private AddOptions options;

	@Override
	public boolean isRediSearch() {
		return true;
	}

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		Double score = convert(item.getOrDefault(scoreField, defaultScore), Double.class);
		return write(commands, redis, index, key, score, stringMap(item), options);
	}

	protected Object write(RedisCommands<R> commands, R redis, String index, String key, double score,
			Map<String, String> map, AddOptions options) {
		return commands.ftadd(redis, index, key, score, map, options);
	}

	@Override
	public String toString() {
		return String.format("RediSearch index %s", index);
	}
}
