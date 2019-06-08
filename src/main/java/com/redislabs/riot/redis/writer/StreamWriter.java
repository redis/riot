package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;

@Setter
public class StreamWriter extends AbstractRedisDataStructureItemWriter {

	private Long maxlen;
	private boolean approximateTrimming;
	private String idField;

	private String id(Map<String, Object> item) {
		return convert(item.remove(idField), String.class);
	}

	@Override
	protected void write(Pipeline pipeline, String key, Map<String, Object> item) {
		Map<String, String> fields = stringMap(item);
		if (idField == null) {
			if (maxlen == null) {
				pipeline.xadd(key, new StreamEntryID(), fields);
			} else {
				pipeline.xadd(key, new StreamEntryID(), fields, maxlen, approximateTrimming);
			}
		} else {
			String id = id(item);
			if (maxlen == null) {
				pipeline.xadd(key, new StreamEntryID(id), fields);
			} else {
				pipeline.xadd(key, new StreamEntryID(id), fields, maxlen, approximateTrimming);
			}
		}
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		Map<String, String> fields = stringMap(item);
		if (idField == null) {
			if (maxlen == null) {
				return commands.xadd(key, fields);
			}
			return commands.xadd(key, fields, maxlen, approximateTrimming);
		}
		XAddArgs args = new XAddArgs();
		args.id(id(item));
		if (maxlen == null) {
			return commands.xadd(key, args, fields);
		}
		return commands.xadd(key, args, fields, maxlen, approximateTrimming);
	}

}