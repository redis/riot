package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.function.StreamMessageIdDropFunction;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.Option;

public class KeyValueProcessorArgs {

	@Option(names = "--key-proc", description = "SpEL template expression to transform key names, e.g. \"#{#source.database}:#{key}\" for 'abc' returns '0:abc'", paramLabel = "<exp>")
	private TemplateExpression keyExpression;

	@Option(names = "--type-proc", description = "SpEL expression to transform key types.", paramLabel = "<exp>")
	private Expression typeExpression;

	@Option(names = "--ttl-proc", description = "SpEL expression to transform key expiration times.", paramLabel = "<exp>")
	private Expression ttlExpression;

	@Option(names = "--ttls", description = "Propagate key expiration times. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean propagateTtls = true;

	@Option(names = "--stream-ids", description = "Propagate stream message IDs. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean propagateStreamMessageIds = true;

	public ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor(EvaluationContext context) {
		List<ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>>> processors = new ArrayList<>();
		if (keyExpression != null) {
			Function<KeyValue<String, Object>, String> function = expressionFunction(context, keyExpression);
			processors.add(processor(function, KeyValue::setKey));
		}
		if (!propagateTtls) {
			processors.add(processor(t -> 0, KeyValue::setTtl));
		}
		if (ttlExpression != null) {
			Function<KeyValue<String, Object>, Long> function = longExpressionFunction(context, ttlExpression);
			processors.add(processor(function, KeyValue::setTtl));
		}
		if (!propagateStreamMessageIds) {
			StreamMessageIdDropFunction function = new StreamMessageIdDropFunction();
			processors.add(processor(function, KeyValue::setValue));
		}
		if (typeExpression != null) {
			Function<KeyValue<String, Object>, String> function = expressionFunction(context, typeExpression);
			processors.add(processor(function, KeyValue::setType));
		}
		return RiotUtils.processor(processors);
	}

	private <T, U> ItemProcessor<T, T> processor(Function<T, U> function, BiConsumer<T, U> consumer) {
		return new ConsumerFunctionItemProcessor<>(function, consumer);
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

	private static class ConsumerFunctionItemProcessor<T, U> implements ItemProcessor<T, T> {

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

	public TemplateExpression getKeyExpression() {
		return keyExpression;
	}

	public void setKeyExpression(TemplateExpression keyExpression) {
		this.keyExpression = keyExpression;
	}

	public Expression getTypeExpression() {
		return typeExpression;
	}

	public void setTypeExpression(Expression typeExpression) {
		this.typeExpression = typeExpression;
	}

	public Expression getTtlExpression() {
		return ttlExpression;
	}

	public void setTtlExpression(Expression ttlExpression) {
		this.ttlExpression = ttlExpression;
	}

	public boolean isPropagateTtls() {
		return propagateTtls;
	}

	public void setPropagateTtls(boolean propagateTtls) {
		this.propagateTtls = propagateTtls;
	}

	public boolean isPropagateStreamMessageIds() {
		return propagateStreamMessageIds;
	}

	public void setPropagateStreamMessageIds(boolean propagateStreamMessageIds) {
		this.propagateStreamMessageIds = propagateStreamMessageIds;
	}

	@Override
	public String toString() {
		return "KeyValueProcessorArgs [keyExpression=" + keyExpression + ", typeExpression=" + typeExpression
				+ ", ttlExpression=" + ttlExpression + ", propagateTtls=" + propagateTtls
				+ ", propagateStreamMessageIds=" + propagateStreamMessageIds + "]";
	}

}
