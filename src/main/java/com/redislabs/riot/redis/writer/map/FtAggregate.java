package com.redislabs.riot.redis.writer.map;

import java.util.List;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Builder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class FtAggregate extends AbstractFtIndexCommandWriter {

	@Builder
	protected FtAggregate(String index, String query, List<String> options) {
		super(index, query, options);
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String index, String query, Object... options) {
		return commands.ftaggregate(redis, index, query, options);
	}

}
