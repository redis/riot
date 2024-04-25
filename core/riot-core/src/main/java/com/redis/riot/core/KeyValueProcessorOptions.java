package com.redis.riot.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import com.redis.riot.core.function.CompositeOperator;
import com.redis.riot.core.function.DropStreamMessageId;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.LongExpressionFunction;
import com.redis.riot.core.function.StringKeyValueFunction;
import com.redis.riot.core.function.ToStringKeyValueFunction;
import com.redis.spring.batch.KeyValue;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueProcessorOptions {

	private TemplateExpression keyExpression;
	private Expression ttlExpression;
	private boolean dropTtl;
	private Expression typeExpression;
	private boolean dropStreamMessageId;

	public <K> FunctionItemProcessor<KeyValue<K, Object>, KeyValue<K, Object>> processor(EvaluationContext ctx,
			RedisCodec<K, ?> codec) {
		if (keyExpression == null && ttlExpression == null && !dropTtl && typeExpression == null
				&& !dropStreamMessageId) {
			return null;
		}
		Function<KeyValue<K, Object>, KeyValue<String, Object>> code = new ToStringKeyValueFunction<>(codec);
		Function<KeyValue<String, Object>, KeyValue<K, Object>> decode = new StringKeyValueFunction<>(codec);
		CompositeOperator<KeyValue<String, Object>> operator = new CompositeOperator<>(processorConsumers(ctx));
		return new FunctionItemProcessor<>(code.andThen(operator).andThen(decode));
	}

	private List<Consumer<KeyValue<String, Object>>> processorConsumers(EvaluationContext context) {
		List<Consumer<KeyValue<String, Object>>> consumers = new ArrayList<>();
		if (keyExpression != null) {
			ExpressionFunction<Object, String> function = new ExpressionFunction<>(context,
					keyExpression.getExpression(), String.class);
			consumers.add(t -> t.setKey(function.apply(t)));
		}
		if (dropTtl) {
			consumers.add(t -> t.setTtl(0));
		}
		if (ttlExpression != null) {
			ToLongFunction<KeyValue<String, Object>> function = new LongExpressionFunction<>(context, ttlExpression);
			consumers.add(t -> t.setTtl(function.applyAsLong(t)));
		}
		if (dropStreamMessageId) {
			consumers.add(new DropStreamMessageId());
		}
		if (typeExpression != null) {
			Function<KeyValue<String, Object>, String> function = new ExpressionFunction<>(context, typeExpression,
					String.class);
			consumers.add(t -> t.setType(function.apply(t)));
		}
		return consumers;
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
