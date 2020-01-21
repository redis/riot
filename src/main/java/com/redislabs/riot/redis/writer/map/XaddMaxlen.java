package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XaddMaxlen extends Xadd {

	@Setter
	private long maxlen;
	@Setter
	private boolean approximateTrimming;

	@Override
	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map, maxlen, approximateTrimming);
	}

}