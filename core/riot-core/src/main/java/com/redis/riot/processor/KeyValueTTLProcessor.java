package com.redis.riot.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyValueTTLProcessor<T extends KeyValue<byte[], ?>> implements ItemProcessor<T, T> {

	private final Expression expression;
	private final EvaluationContext context;

	public KeyValueTTLProcessor(Expression expression, EvaluationContext context) {
		this.expression = expression;
		this.context = context;
	}

	@Override
	public T process(T item) {
		KeyValue<String, ?> stringKeyValue = new KeyValue<>(
				StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey(item.getKey())), item.getValue());
		item.setTtl(expression.getValue(context, stringKeyValue, Long.class));
		return item;
	}

}