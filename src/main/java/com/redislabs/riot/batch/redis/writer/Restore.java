package com.redislabs.riot.batch.redis.writer;

import com.redislabs.riot.batch.redis.KeyValue;
import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class Restore<R> extends AbstractRedisWriter<R, KeyValue> {

	@Override
	protected Object write(RedisCommands<R> commands, R redis, KeyValue item) throws Exception {
		return commands.restore(redis, item.getKey(), filter(item.getTtl()), item.getValue());
	}

	private long filter(long ttl) {
		if (ttl == -1) {
			return 0;
		}
		return ttl;
	}

}