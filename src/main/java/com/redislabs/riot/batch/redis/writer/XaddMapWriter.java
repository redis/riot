package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;

@SuppressWarnings("unchecked")
public class XaddMapWriter extends AbstractRedisFlatMapWriter {

	@Override
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.xadd(key, null, stringMap(item));
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.xadd(key, null, stringMap(item));
	}

	@Override
	protected RedisFuture<?> write(Object commands, String key, Map<String, Object> item) {
		return ((RedisStreamAsyncCommands<String, String>) commands).xadd(key, stringMap(item));
	}

}