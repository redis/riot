package com.redislabs.riot.redis.writer.map;

import com.redislabs.riot.redis.KeyDump;
import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractCommandWriter;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class Restore<R> extends AbstractCommandWriter<KeyDump> {

	@Setter
	private boolean replace;

	@Override
	protected Object write(RedisCommands commands, Object redis, KeyDump item) throws Exception {
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