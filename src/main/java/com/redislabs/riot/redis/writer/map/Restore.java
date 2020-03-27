package com.redislabs.riot.redis.writer.map;

import com.redislabs.riot.redis.KeyDump;
import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractCommandWriter;

import lombok.Builder;
import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Builder
public class Restore<R> extends AbstractCommandWriter<KeyDump> {

	private @Setter boolean replace;

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