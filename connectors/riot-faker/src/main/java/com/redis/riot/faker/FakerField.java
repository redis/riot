package com.redis.riot.faker;

import org.springframework.expression.Expression;

import com.redis.riot.core.RiotUtils;

public class FakerField {

	private String name;
	private Expression expression;

	public String getName() {
		return name;
	}

	public void setName(String field) {
		this.name = field;
	}

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	public static FakerField of(String name, String expression) {
		return of(name, RiotUtils.parse(expression));
	}

	public static FakerField of(String name, Expression expression) {
		FakerField field = new FakerField();
		field.setName(name);
		field.setExpression(expression);
		return field;
	}

}
