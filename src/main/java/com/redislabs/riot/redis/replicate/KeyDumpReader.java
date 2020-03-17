package com.redislabs.riot.redis.replicate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.redislabs.riot.redis.KeyDump;
import com.redislabs.riot.redis.ValueReader;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public @Data class KeyDumpReader implements ValueReader<KeyDump> {

	private long timeout;

	@SuppressWarnings("unchecked")
	@Override
	public KeyDump[] fetch(List<String> keys, BaseRedisAsyncCommands<String, String> commands) {
		KeyDump[] values = new KeyDump[keys.size()];
		commands.setAutoFlushCommands(false);
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<byte[]>> valueFutures = new ArrayList<>(keys.size());
		for (String key : keys) {
			ttlFutures.add(((RedisKeyAsyncCommands<String, String>) commands).pttl(key));
			valueFutures.add(((RedisKeyAsyncCommands<String, String>) commands).dump(key));
		}
		commands.flushCommands();
		for (int index = 0; index < keys.size(); index++) {
			try {
				values[index] = KeyDump.builder().key(keys.get(index))
						.ttl(ttlFutures.get(index).get(timeout, TimeUnit.SECONDS))
						.value(valueFutures.get(index).get(timeout, TimeUnit.SECONDS)).build();
			} catch (Exception e) {
				log.error("Could not read value for key {}", keys.get(index), e);
				continue;
			}
		}
		return values;
	}

}
