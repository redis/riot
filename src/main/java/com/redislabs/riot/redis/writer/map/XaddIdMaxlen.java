package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class XaddIdMaxlen extends XaddId {

	@Setter
	private long maxlen;
	@Setter
	private boolean approximateTrimming;

	@Override
	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map, String id) {
		return commands.xadd(redis, key, id, map, maxlen, approximateTrimming);
	}

}