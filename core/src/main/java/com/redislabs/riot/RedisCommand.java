package com.redislabs.riot;

import io.lettuce.core.RedisFuture;

import java.util.function.BiFunction;

public interface RedisCommand<T> {

	BiFunction<?, T, RedisFuture<?>> command();
}
