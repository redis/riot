package com.redislabs.riot.redis.writer.map;

import java.util.List;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Builder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class FtSearch extends AbstractFtIndexCommandWriter {

	@Builder
	protected FtSearch(String index, String query, List<String> options) {
		super(index, query, options);
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String index, String query, Object... options) {
		return commands.ftsearch(redis, index, query, options);
	}

}
