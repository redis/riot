package com.redis.riot.processor;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

abstract class AbstractKeyValueProcessor<T extends KeyValue<byte[], ?>> implements ItemProcessor<T, T> {

	@Override
	public T process(T item) {
		KeyValue<String, Object> kv = new KeyValue<>();
		kv.setKey(StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(item.getKey())));
		kv.setValue(item.getValue());
		process(item, kv);
		return item;
	}

	protected abstract void process(T item, KeyValue<String, Object> kv);

}