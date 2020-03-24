package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Xadd extends AbstractKeyMapCommandWriter {

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		return doWrite(commands, redis, key, stringMap(item));
	}

	protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
		return commands.xadd(redis, key, map);
	}

	@Accessors(fluent = true)
	public static class XaddId extends Xadd {

		private @Setter String id;

		@Override
		protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
			return doWrite(commands, redis, key, map, map.remove(id));
		}

		protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map, String id) {
			return commands.xadd(redis, key, id, map);
		}

	}

	@Accessors(fluent = true)
	public static class XaddIdMaxlen extends XaddId {

		private @Setter long maxlen;
		private @Setter boolean approximateTrimming;

		@Override
		protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map, String id) {
			return commands.xadd(redis, key, id, map, maxlen, approximateTrimming);
		}

	}

	@Accessors(fluent = true)
	public static class XaddMaxlen extends Xadd {

		private @Setter long maxlen;
		private @Setter boolean approximateTrimming;

		@Override
		protected Object doWrite(RedisCommands commands, Object redis, String key, Map<String, String> map) {
			return commands.xadd(redis, key, map, maxlen, approximateTrimming);
		}

	}

}