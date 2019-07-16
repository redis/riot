package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class ExpireWriter extends AbstractRedisDataStructureItemWriter {

	private String timeoutField;
	private Long defaultTimeout;

	public ExpireWriter(String timeoutField, Long defaultTimeout) {
		this.timeoutField = timeoutField;
		this.defaultTimeout = defaultTimeout;
	}

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.expire(key, Math.toIntExact(timeout(item)));
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.expire(key, timeout(item));
	}

	private long timeout(Map<String, Object> item) {
		return convert(item.getOrDefault(timeoutField, defaultTimeout), Long.class);
	}

}