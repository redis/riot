package com.redis.riot.function;

import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValue<K> implements Function<KeyValue<K>, KeyValue<String>> {

	private final Function<K, String> toStringKeyFunction;

	public ToStringKeyValue(RedisCodec<K, ?> codec) {
		this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@Override
	public KeyValue<String> apply(KeyValue<K> item) {
		KeyValue<String> keyValue = new KeyValue<>();
		keyValue.setKey(toStringKeyFunction.apply(item.getKey()));
		keyValue.setEvent(item.getEvent());
		keyValue.setMemoryUsage(item.getMemoryUsage());
		keyValue.setTimestamp(item.getTimestamp());
		keyValue.setTtl(item.getTtl());
		keyValue.setType(item.getType());
		keyValue.setValue(item.getValue());
		return keyValue;
	}

}
