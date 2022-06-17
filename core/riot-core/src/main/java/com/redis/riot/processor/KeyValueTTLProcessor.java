package com.redis.riot.processor;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.spring.batch.KeyValue;

public class KeyValueTTLProcessor<T extends KeyValue<byte[], ?>> extends AbstractKeyValueProcessor<T> {

	private final Expression expression;
	private final EvaluationContext context;

	public KeyValueTTLProcessor(Expression expression, EvaluationContext context) {
		this.expression = expression;
		this.context = context;
	}

	@Override
	protected void process(T item, KeyValue<String, Object> kv) {
		item.setTtl(expression.getValue(context, kv, Long.class));
	}

}