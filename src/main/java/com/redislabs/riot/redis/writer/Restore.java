package com.redislabs.riot.redis.writer;

import com.redislabs.riot.redis.KeyValue;
import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class Restore<R> extends AbstractRedisWriter<R, KeyValue> {

	@Setter
	private boolean replace;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, KeyValue item) throws Exception {
		byte[] value = item.value();
		if (value == null) {
			// DUMP returns null if key does not exist
			return commands.del(redis, item.key());
		}
		long ttl = filter(item.ttl());
		return commands.restore(redis, item.key(), item.value(), ttl, replace);
	}

	private long filter(long ttl) {
		if (ttl == -1) {
			return 0;
		}
		return ttl;
	}

}