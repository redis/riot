package com.redis.riot.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.StreamMessageIdDropFunction;
import com.redis.riot.core.function.StringKeyValueFunction;
import com.redis.riot.core.function.ToStringKeyValueProcessor;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueProcessorOptions {

	private TemplateExpression keyExpression;
	private Expression ttlExpression;
	private boolean dropTtl;
	private Expression typeExpression;
	private boolean dropStreamMessageId;

	public <K> ItemProcessor<KeyValue<K, Object>, KeyValue<K, Object>> processor(EvaluationContext evaluationContext,
			RedisCodec<K, ?> codec) {
		if (isEmpty()) {
			return null;
		}
		ItemProcessor<KeyValue<K, Object>, KeyValue<String, Object>> code = new ToStringKeyValueProcessor<>(codec);
		ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor = processor(evaluationContext);
		ItemProcessor<KeyValue<String, Object>, KeyValue<K, Object>> decode = new StringKeyValueFunction<>(codec);
		return RiotUtils.processor(code, processor, decode);
	}

	private boolean isEmpty() {
		return !(hasKeyExpression() || hasTtlExpression() || isDropTtl() || hasTypeExpression()
				|| isDropStreamMessageId());
	}

	private boolean hasTypeExpression() {
		return typeExpression != null;
	}

	private boolean hasTtlExpression() {
		return ttlExpression != null;
	}

	private boolean hasKeyExpression() {
		return keyExpression != null;
	}

	public ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor(EvaluationContext context) {
		List<ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>>> processors = new ArrayList<>();
		if (hasKeyExpression()) {
			processors.add(
					new ConsumerFunctionItemProcessor<>(expressionFunction(context, keyExpression), KeyValue::setKey));
		}
		if (isDropTtl()) {
			processors.add(new ConsumerFunctionItemProcessor<>(t -> 0, KeyValue::setTtl));
		}
		if (hasTtlExpression()) {
			processors.add(new ConsumerFunctionItemProcessor<>(longExpressionFunction(context, ttlExpression),
					KeyValue::setTtl));
		}
		if (isDropStreamMessageId()) {
			processors.add(new ConsumerFunctionItemProcessor<>(new StreamMessageIdDropFunction(), KeyValue::setValue));
		}
		if (hasTypeExpression()) {
			processors.add(new ConsumerFunctionItemProcessor<>(expressionFunction(context, typeExpression),
					KeyValue::setType));
		}
		return RiotUtils.processor(processors);
	}

	private Function<KeyValue<String, Object>, String> expressionFunction(EvaluationContext context,
			TemplateExpression expression) {
		return expressionFunction(context, expression.getExpression());
	}

	private Function<KeyValue<String, Object>, String> expressionFunction(EvaluationContext context,
			Expression expression) {
		return new ExpressionFunction<>(context, expression, String.class);
	}

	private Function<KeyValue<String, Object>, Long> longExpressionFunction(EvaluationContext context,
			Expression expression) {
		return new ExpressionFunction<>(context, expression, Long.class);
	}

	private class ConsumerFunctionItemProcessor<T, U> implements ItemProcessor<T, T> {

		private final BiConsumer<T, U> consumer;
		private final Function<T, U> function;

		public ConsumerFunctionItemProcessor(Function<T, U> function, BiConsumer<T, U> consumer) {
			this.function = function;
			this.consumer = consumer;
		}

		@Override
		public T process(T item) throws Exception {
			consumer.accept(item, function.apply(item));
			return item;
		}

	}

	public boolean isDropStreamMessageId() {
		return dropStreamMessageId;
	}

	public void setDropStreamMessageId(boolean dropStreamMessageId) {
		this.dropStreamMessageId = dropStreamMessageId;
	}

	public Expression getTypeExpression() {
		return typeExpression;
	}

	public void setTypeExpression(Expression expression) {
		this.typeExpression = expression;
	}

	public boolean isDropTtl() {
		return dropTtl;
	}

	public void setDropTtl(boolean dropTtl) {
		this.dropTtl = dropTtl;
	}

	public TemplateExpression getKeyExpression() {
		return keyExpression;
	}

	public void setKeyExpression(TemplateExpression expression) {
		this.keyExpression = expression;
	}

	public Expression getTtlExpression() {
		return ttlExpression;
	}

	public void setTtlExpression(Expression expression) {
		this.ttlExpression = expression;
	}

}
