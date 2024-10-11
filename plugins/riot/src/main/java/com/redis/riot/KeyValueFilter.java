package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueFilter<K, T extends KeyValue<K>> implements ItemProcessor<T, T> {

	private final Function<K, String> keyToString;
	private final Logger log;

	public KeyValueFilter(RedisCodec<K, ?> codec, Logger log) {
		this.keyToString = BatchUtils.toStringKeyFunction(codec);
		this.log = log;
	}

	@Override
	public T process(T item) throws Exception {
		if (KeyValue.exists(item) && !KeyValue.hasValue(item) && item.getMemoryUsage() > 0) {
			if (log.isInfoEnabled()) {
				DataSize memUsage = DataSize.ofBytes(item.getMemoryUsage());
				log.info("Skipping {} {} ({})", item.getType(), string(item.getKey()), memUsage);
			}
			return null;
		}
		return item;
	}

	private String string(K key) {
		return keyToString.apply(key);
	}

}
