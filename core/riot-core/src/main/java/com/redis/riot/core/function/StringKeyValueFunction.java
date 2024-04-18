package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class StringKeyValueFunction<K, T> implements Function<KeyValue<String, T>, KeyValue<K, T>> {

	private final Function<String, K> stringKeyFunction;

	public StringKeyValueFunction(RedisCodec<K, ?> codec) {
		this.stringKeyFunction = BatchUtils.stringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<K, T> apply(KeyValue<String, T> t) {
		KeyValue<K, T> result = new KeyValue<>((KeyValue) t);
		result.setKey(stringKeyFunction.apply(t.getKey()));
		return result;
	}

}
