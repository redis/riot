package com.redis.riot.function;

import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class StringKeyValueFunction<K> implements ItemProcessor<KeyValue<String, Object>, KeyValue<K, Object>> {

	private final Function<String, K> stringKeyFunction;

	public StringKeyValueFunction(RedisCodec<K, ?> codec) {
		this.stringKeyFunction = BatchUtils.stringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<K, Object> process(KeyValue<String, Object> item) {
		KeyValue<K, Object> result = new KeyValue<>((KeyValue) item);
		result.setKey(stringKeyFunction.apply(item.getKey()));
		return result;
	}

}
