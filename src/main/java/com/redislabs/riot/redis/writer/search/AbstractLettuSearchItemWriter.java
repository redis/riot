package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.LettuceItemWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.Setter;
import reactor.core.publisher.Mono;

public abstract class AbstractLettuSearchItemWriter extends AbstractRedisItemWriter implements LettuceItemWriter {

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
	public RedisFuture<?> write(RedisAsyncCommands<String, String> commands, Map<String, Object> item) {
		return write((RediSearchAsyncCommands<String, String>) commands, index, item);
	}

	protected abstract RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item);

	@Override
	public Mono<?> write(RedisReactiveCommands<String, String> commands, Map<String, Object> item) {
		return write((RediSearchReactiveCommands<String, String>) commands, index, item);
	}

	protected abstract Mono<?> write(RediSearchReactiveCommands<String, String> commands, String index,
			Map<String, Object> item);

}
