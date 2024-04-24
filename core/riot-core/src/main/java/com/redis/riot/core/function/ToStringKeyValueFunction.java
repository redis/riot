package com.redis.riot.core.function;

import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValueFunction<K> implements Function<KeyValue<K, Object>, KeyValue<String, Object>> {

	private final Function<K, String> toStringKeyFunction;

	public ToStringKeyValueFunction(RedisCodec<K, ?> codec) {
		this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<String, Object> apply(KeyValue<K, Object> t) {
		KeyValue<String, Object> result = new KeyValue<>((KeyValue) t);
		result.setKey(toStringKeyFunction.apply(t.getKey()));
		return result;
	}

}
