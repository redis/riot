package com.redislabs.riot.redis.writer;

import com.redislabs.riot.redis.KeyValue;
import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

public class Restore<R> extends AbstractRedisWriter<R, KeyValue> {

	@Setter
	private boolean replace;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, KeyValue item) throws Exception {
		byte[] value = item.getValue();
		if (value == null) {
			// DUMP returns null if key does not exist
			return commands.del(redis, item.getKey());
		}
		long ttl = filter(item.getTtl());
		return commands.restore(redis, item.getKey(), item.getValue(), ttl, replace);
	}

	private long filter(long ttl) {
		if (ttl == -1) {
			return 0;
		}
		return ttl;
	}

}