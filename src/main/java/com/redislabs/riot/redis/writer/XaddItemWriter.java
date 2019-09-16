package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;

public class XaddItemWriter extends RedisItemWriter<RedisStreamAsyncCommands<String, String>> {

	@Override
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.xadd(key, null, stringMap(item));
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.xadd(key, null, stringMap(item));
	}

	@Override
	protected RedisFuture<?> write(RedisStreamAsyncCommands<String, String> commands, String key,
			Map<String, Object> item) {
		return commands.xadd(key, stringMap(item));
	}

}