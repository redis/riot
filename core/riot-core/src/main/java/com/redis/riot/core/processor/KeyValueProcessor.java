package com.redis.riot.core.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyValueProcessor implements ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> {

	private final Expression expression;
	private final EvaluationContext context;

	public KeyValueProcessor(Expression expression, EvaluationContext context) {
		this.expression = expression;
		this.context = context;
	}

	@Override
	public KeyValue<byte[]> process(KeyValue<byte[]> item) throws Exception {
		KeyValue<String> keyValue = new KeyValue<>();
		keyValue.setKey(encodeKey(item.getKey()));
		keyValue.setMemoryUsage(item.getMemoryUsage());
		keyValue.setTtl(item.getTtl());
		keyValue.setType(item.getType());
		String key = expression.getValue(context, keyValue, String.class);
		item.setKey(decodeKey(key));
		return item;
	}

	private String encodeKey(byte[] key) {
		return StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(key));
	}

	private byte[] decodeKey(String key) {
		return ByteArrayCodec.INSTANCE.decodeKey(StringCodec.UTF8.encodeKey(key));
	}

}