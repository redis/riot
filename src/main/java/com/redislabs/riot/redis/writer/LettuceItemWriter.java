package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface LettuceItemWriter<C extends RedisAsyncCommands<String, String>> {

	RedisFuture<?> write(C commands, Map<String, Object> item);

}
