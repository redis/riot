package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;

public interface LettuceMapWriter<C> {

	RedisFuture<?> write(C commands, Map<String, Object> item);

}
