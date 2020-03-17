package com.redislabs.riot.redis.reader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.redislabs.riot.redis.ValueReader;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
@SuppressWarnings({ "unchecked", "rawtypes" })
public @Data class KeyValueReader implements ValueReader<KeyValue> {

	private long timeout;

	@Override
	public KeyValue[] fetch(List<String> keys, BaseRedisAsyncCommands<String, String> commands) {
		commands.setAutoFlushCommands(false);
		RedisFuture<String>[] typeFutures = new RedisFuture[keys.size()];
		for (int index = 0; index < keys.size(); index++) {
			typeFutures[index] = ((RedisKeyAsyncCommands<String, String>) commands).type(keys.get(index));
		}
		commands.flushCommands();
		KeyValue[] values = new KeyValue[keys.size()];
		for (int index = 0; index < keys.size(); index++) {
			try {
				Type type = Type.valueOf(typeFutures[index].get(timeout, TimeUnit.SECONDS).toUpperCase());
				values[index] = KeyValue.builder().key(keys.get(index)).type(type).build();
			} catch (Exception e) {
				log.error("Could not get type of key {}", keys.get(index), e);
			}
		}
		RedisFuture[] valueFutures = new RedisFuture[keys.size()];
		for (int index = 0; index < keys.size(); index++) {
			valueFutures[index] = getValue(keys.get(index), values[index].getType(), commands);
		}
		commands.flushCommands();
		for (int index = 0; index < keys.size(); index++) {
			try {
				values[index].setValue(valueFutures[index].get(timeout, TimeUnit.SECONDS));
			} catch (Exception e) {
				log.error("Could not get value for {} {}", values[index].getType(), values[index].getKey(), e);
			}
		}
		return values;
	}

	private RedisFuture getValue(String key, Type type, BaseRedisAsyncCommands<String, String> commands) {
		switch (type) {
		case STRING:
			return ((RedisStringAsyncCommands<String, String>) commands).get(key);
		case LIST:
			return ((RedisListAsyncCommands<String, String>) commands).lrange(key, 0, -1);
		case SET:
			return ((RedisSetAsyncCommands<String, String>) commands).smembers(key);
		case ZSET:
			return ((RedisSortedSetAsyncCommands<String, String>) commands).zrangeWithScores(key, 0, -1);
		case HASH:
			return ((RedisHashAsyncCommands<String, String>) commands).hgetall(key);
		case STREAM:
			return ((RedisStreamAsyncCommands<String, String>) commands).xrange(key, Range.<String>create("-", "+"));
		}
		return null;
	}
}
