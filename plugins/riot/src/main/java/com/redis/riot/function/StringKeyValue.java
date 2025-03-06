package com.redis.riot.function;

import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class StringKeyValue<K> implements Function<KeyValue<String>, KeyValue<K>> {

	private final Function<String, K> stringKeyFunction;

	public StringKeyValue(RedisCodec<K, ?> codec) {
		this.stringKeyFunction = BatchUtils.stringKeyFunction(codec);
	}

	@Override
	public KeyValue<K> apply(KeyValue<String> item) {
		KeyValue<K> keyValue = new KeyValue<>();
		keyValue.setEvent(item.getEvent());
		keyValue.setKey(stringKeyFunction.apply(item.getKey()));
		keyValue.setMemoryUsage(item.getMemoryUsage());
		keyValue.setTimestamp(item.getTimestamp());
		keyValue.setTtl(item.getTtl());
		keyValue.setType(item.getType());
		keyValue.setValue(item.getValue());
		return keyValue;
	}

}
