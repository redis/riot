package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;

public abstract class AbstractLettuSearchItemWriter extends AbstractRedisItemWriter implements LettuceItemWriter {

	@Setter
	private String index;

	@Override
	public RedisFuture<?> write(RedisAsyncCommands<String, String> commands, Map<String, Object> item) {
		return write((RediSearchAsyncCommands<String, String>) commands, index, item);
	}

	protected abstract RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item);

}
