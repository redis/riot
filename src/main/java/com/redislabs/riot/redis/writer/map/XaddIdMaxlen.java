package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class XaddIdMaxlen<R> extends XaddId<R> {

	@Setter
	private long maxlen;
	@Setter
	private boolean approximateTrimming;

	@Override
	protected Object doWrite(RedisCommands<R> commands, R redis, String key, Map<String, String> map, String id) {
		return commands.xadd(redis, key, id, map, maxlen, approximateTrimming);
	}

}