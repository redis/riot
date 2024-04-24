package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class StringKeyValueFunction<K> implements Function<KeyValue<String, Object>, KeyValue<K, Object>> {

	private final Function<String, K> stringKeyFunction;

	public StringKeyValueFunction(RedisCodec<K, ?> codec) {
		this.stringKeyFunction = BatchUtils.stringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<K, Object> apply(KeyValue<String, Object> t) {
		KeyValue<K, Object> result = new KeyValue<>((KeyValue) t);
		result.setKey(stringKeyFunction.apply(t.getKey()));
		return result;
	}

}
