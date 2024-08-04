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
	public KeyValue apply(KeyValue item) {
		item.setKey(toStringKeyFunction.apply((K) item.getKey()));
		return item;
	}

}
