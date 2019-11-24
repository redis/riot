package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class FtAddPayload<R> extends FtAdd<R> {

	@Setter
	private String payload;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String index, String key, double score,
			Map<String, String> map, AddOptions options) {
		return commands.ftadd(redis, index, key, score, map, options, map.remove(payload));
	}

}
