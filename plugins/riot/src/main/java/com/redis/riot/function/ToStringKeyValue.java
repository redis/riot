package com.redis.riot.function;

import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValue<K> implements Function<KeyValue<K, Object>, KeyValue<String, Object>> {

	private final Function<K, String> toStringKeyFunction;

	public ToStringKeyValue(RedisCodec<K, ?> codec) {
		this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<String, Object> apply(KeyValue<K, Object> item) {
		KeyValue<String, Object> result = new KeyValue<>((KeyValue) item);
		result.setKey(toStringKeyFunction.apply(item.getKey()));
		return result;
	}

}
