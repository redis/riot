package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class XaddMaxlen<R> extends Xadd<R> {

	@Setter
	private long maxlen;
	@Setter
	private boolean approximateTrimming;

	@Override
	protected Object doWrite(RedisCommands<R> commands, R redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map, maxlen, approximateTrimming);
	}

}