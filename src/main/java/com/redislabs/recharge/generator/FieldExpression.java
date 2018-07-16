package com.redislabs.recharge.generator;

import org.springframework.expression.Expression;

import lombok.Data;

@Data
public class FieldExpression {
	private String field;
	private Expression expression;

}
