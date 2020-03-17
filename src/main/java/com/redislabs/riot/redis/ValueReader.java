package com.redislabs.riot.redis;

import java.util.List;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface ValueReader<T> {

	T[] fetch(List<String> keys, BaseRedisAsyncCommands<String, String> commands);

}
