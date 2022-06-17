package com.redis.riot.processor;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyValueKeyProcessor<T extends KeyValue<byte[], ?>> extends AbstractKeyValueProcessor<T> {

	private final Expression expression;
	private final EvaluationContext context;

	public KeyValueKeyProcessor(Expression expression, EvaluationContext context) {
		this.expression = expression;
		this.context = context;
	}

	@Override
	protected void process(T item, KeyValue<String, Object> kv) {
		String key = expression.getValue(context, kv, String.class);
		item.setKey(ByteArrayCodec.INSTANCE.decodeKey(StringCodec.UTF8.encodeKey(key)));
	}

}