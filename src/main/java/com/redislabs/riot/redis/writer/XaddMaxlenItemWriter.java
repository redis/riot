package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;

public class XaddMaxlenItemWriter extends RedisItemWriter<RedisStreamAsyncCommands<String, String>> {

	private Long maxlen;
	private boolean approximateTrimming;

	public void setMaxlen(Long maxlen) {
		this.maxlen = maxlen;
	}

	public void setApproximateTrimming(boolean approximateTrimming) {
		this.approximateTrimming = approximateTrimming;
	}

	@Override
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.xadd(key, null, stringMap(item), maxlen, approximateTrimming);
	}
	
	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.xadd(key, null, stringMap(item), maxlen, approximateTrimming);
	}

	@Override
	protected RedisFuture<?> write(RedisStreamAsyncCommands<String, String> commands, String key,
			Map<String, Object> item) {
		return commands.xadd(key, stringMap(item), maxlen, approximateTrimming);
	}

}