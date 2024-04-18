package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValueFunction<K, T> implements Function<KeyValue<K, T>, KeyValue<String, T>> {

	private final Function<K, String> toStringKeyFunction;

	public ToStringKeyValueFunction(RedisCodec<K, ?> codec) {
		this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<String, T> apply(KeyValue<K, T> t) {
		KeyValue<String, T> result = new KeyValue<>((KeyValue) t);
		result.setKey(toStringKeyFunction.apply(t.getKey()));
		return result;
	}

}
