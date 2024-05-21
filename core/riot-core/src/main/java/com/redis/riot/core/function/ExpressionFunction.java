package com.redis.riot.core.function;

import java.util.function.Function;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.util.Assert;

public class ExpressionFunction<T, R> implements Function<T, R> {

	private final EvaluationContext context;

	private final Expression expression;

	private final Class<R> type;

	public ExpressionFunction(EvaluationContext context, Expression expression, Class<R> type) {
		Assert.notNull(context, "A SpEL evaluation context is required.");
		Assert.notNull(expression, "A SpEL expression is required.");
		Assert.notNull(type, "A type is required.");
		this.context = context;
		this.expression = expression;
		this.type = type;
	}

	@Override
	public R apply(T t) {
		R value = getValue(t);
		return value;
	}

	protected R getValue(T t) {
		return expression.getValue(context, t, type);
	}

}
