package com.redis.riot;

import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class ToStringKeyValueProcessor<K> implements ItemProcessor<KeyValue<K, Object>, KeyValue<String, Object>> {

	private final Function<K, String> toStringKeyFunction;

	public ToStringKeyValueProcessor(RedisCodec<K, ?> codec) {
		this.toStringKeyFunction = BatchUtils.toStringKeyFunction(codec);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<String, Object> process(KeyValue<K, Object> item) throws Exception {
		KeyValue<String, Object> result = new KeyValue<>((KeyValue) item);
		result.setKey(toStringKeyFunction.apply(item.getKey()));
		return result;
	}

}
