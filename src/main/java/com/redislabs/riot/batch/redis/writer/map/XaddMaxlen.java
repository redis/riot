package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
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