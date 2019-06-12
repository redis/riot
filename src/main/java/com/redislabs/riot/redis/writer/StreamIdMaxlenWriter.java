package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.Setter;
import reactor.core.publisher.Mono;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;

@Setter
public class StreamIdMaxlenWriter extends AbstractRedisDataStructureItemWriter {

	private Long maxlen;
	private boolean approximateTrimming;
	private String idField;

	private String id(Map<String, Object> item) {
		return convert(item.remove(idField), String.class);
	}

	@Override
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.xadd(key, new StreamEntryID(id(item)), stringMap(item), maxlen, approximateTrimming);
	}

	private XAddArgs xAddArgs(Map<String, Object> item) {
		return new XAddArgs().id(id(item));
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.xadd(key, xAddArgs(item), stringMap(item), maxlen, approximateTrimming);
	}

	@Override
	protected Mono<?> write(RedisReactiveCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.xadd(key, xAddArgs(item), stringMap(item), maxlen, approximateTrimming);
	}

}