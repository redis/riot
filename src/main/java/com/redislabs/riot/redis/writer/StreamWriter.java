package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
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
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		StreamEntryID id = streamEntryID(item);
		Map<String, String> fields = stringMap(item);
		if (maxlen == null) {
			return pipeline.xadd(key, id, fields);
		}
		return pipeline.xadd(key, id, fields, maxlen, approximateTrimming);
	}

	private StreamEntryID streamEntryID(Map<String, Object> item) {
		if (idField == null) {
			return null;
		}
		return new StreamEntryID(id(item));
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