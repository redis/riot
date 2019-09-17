package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class ExpireMapWriter extends RedisDataStructureMapWriter<RedisKeyAsyncCommands<String, String>> {

	private String timeoutField;
	private Long defaultTimeout;

	public void setTimeoutField(String timeoutField) {
		this.timeoutField = timeoutField;
	}

	public void setDefaultTimeout(Long defaultTimeout) {
		this.defaultTimeout = defaultTimeout;
	}

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.expire(key, Math.toIntExact(timeout(item)));
	}
	
	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.expire(key, Math.toIntExact(timeout(item)));
	}

	@Override
	protected RedisFuture<?> write(RedisKeyAsyncCommands<String, String> commands, String key,
			Map<String, Object> item) {
		return commands.expire(key, timeout(item));
	}

	private long timeout(Map<String, Object> item) {
		return convert(item.getOrDefault(timeoutField, defaultTimeout), Long.class);
	}

}