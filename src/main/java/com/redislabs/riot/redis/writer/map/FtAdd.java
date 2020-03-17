package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class FtAdd extends AbstractSearchMapCommandWriter {

	@Setter
	private String index;
	@Setter
	private AddOptions options;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return write(commands, redis, index, key, getScore(item), stringMap(item), options);
	}

	protected Object write(RedisCommands commands, Object redis, String index, String key, double score,
			Map<String, String> map, AddOptions options) {
		return commands.ftadd(redis, index, key, score, map, options);
	}

	@Override
	public String toString() {
		return String.format("RediSearch index %s", index);
	}

	public static class FtAddPayload extends FtAdd {

		@Setter
		private String payload;

		@Override
		protected Object write(RedisCommands commands, Object redis, String index, String key, double score,
				Map<String, String> map, AddOptions options) {
			return commands.ftadd(redis, index, key, score, map, options, map.remove(payload));
		}

	}

}
