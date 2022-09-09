package com.redis.riot.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

@SuppressWarnings("rawtypes")
public class KeyValueProcessor<T extends KeyValue> implements ItemProcessor<T, T> {

	private final Expression expression;
	private final EvaluationContext context;

	public KeyValueProcessor(Expression expression, EvaluationContext context) {
		this.expression = expression;
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T process(T item) throws Exception {
		item.setKey(StringCodec.UTF8.decodeKey(ByteArrayCodec.INSTANCE.encodeKey((byte[]) item.getKey())));
		String key = expression.getValue(context, item, String.class);
		item.setKey(ByteArrayCodec.INSTANCE.decodeKey(StringCodec.UTF8.encodeKey(key)));
		return item;
	}

}