package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import io.lettuce.core.RedisFuture;
import lombok.Setter;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public abstract class AbstractLettuSearchItemWriter
		extends AbstractRedisItemWriter<RediSearchAsyncCommands<String, String>> {

	@Setter
	private String index;
	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;

	protected double score(Map<String, Object> item) {
		return convert(item.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	public RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, Map<String, Object> item) {
		return write(commands, index, item);
	}

	protected abstract RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item);

	@Override
	public Response<?> write(Pipeline pipeline, Map<String, Object> item) {
		// not supported
		return null;
	}

}
