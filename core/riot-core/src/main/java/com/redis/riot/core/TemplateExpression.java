package com.redis.riot.core;

import org.springframework.expression.EvaluationContext;

public class TemplateExpression extends Expression {

	public TemplateExpression(org.springframework.expression.Expression expression) {
		super(expression);
	}

	@Override
	public String getValue(EvaluationContext context) {
		return spelExpression.getValue(context, String.class);
	}

	@Override
	public String getValue(EvaluationContext context, Object rootObject) {
		return spelExpression.getValue(context, rootObject, String.class);
	}

}
