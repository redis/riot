package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;

public class StreamMaxlenWriter extends AbstractRedisDataStructureItemWriter {

	private Long maxlen;
	private boolean approximateTrimming;

	public StreamMaxlenWriter(Long maxlen, boolean approximateTrimming) {
		super();
		this.maxlen = maxlen;
		this.approximateTrimming = approximateTrimming;
	}

	@Override
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.xadd(key, null, stringMap(item), maxlen, approximateTrimming);
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.xadd(key, stringMap(item), maxlen, approximateTrimming);
	}

}