package com.redis.riot.core;

import java.util.function.Predicate;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public class Expression {

	protected static final SpelExpressionParser PARSER = new SpelExpressionParser();

	protected final org.springframework.expression.Expression spelExpression;

	public Expression(org.springframework.expression.Expression expression) {
		this.spelExpression = expression;
	}

	@Override
	public String toString() {
		return spelExpression.getExpressionString();
	}

	public <T> Predicate<T> predicate(EvaluationContext context) {
		return t -> spelExpression.getValue(context, t, Boolean.class);
	}

	public Object getValue(EvaluationContext context) {
		return spelExpression.getValue(context);
	}

	public Object getValue(EvaluationContext context, Object rootObject) {
		return spelExpression.getValue(context, rootObject);
	}

	public Long getLong(EvaluationContext context, Object rootObject) {
		return spelExpression.getValue(context, rootObject, Long.class);
	}

	public String getString(EvaluationContext context, Object rootObject) {
		return spelExpression.getValue(context, rootObject, String.class);
	}

	public static Expression parse(String expression) {
		return new Expression(PARSER.parseExpression(expression));
	}

	public static TemplateExpression parseTemplate(String expression) {
		return new TemplateExpression(PARSER.parseExpression(expression, new TemplateParserContext()));
	}

}
