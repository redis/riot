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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue apply(KeyValue item) {
		item.setKey(stringKeyFunction.apply((String) item.getKey()));
		return item;
	}

}
