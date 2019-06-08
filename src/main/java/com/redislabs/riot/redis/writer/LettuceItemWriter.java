package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface LettuceItemWriter {

	RedisFuture<?> write(RedisAsyncCommands<String, String> commands, Map<String, Object> item);

}
