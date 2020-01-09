package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class FtAddPayload<R> extends FtAdd<R> {

	@Setter
	private String payload;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String index, String key, double score,
			Map<String, String> map, AddOptions options) {
		return commands.ftadd(redis, index, key, score, map, options, map.remove(payload));
	}

}
