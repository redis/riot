package com.redislabs.recharge.generator;

import java.util.Map;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import lombok.Setter;

@Setter
public class ExpressionMapGenerator implements MapGenerator {

	private Expression expression;

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> generate(EvaluationContext evaluationContext) {
		return expression.getValue(evaluationContext, Map.class);
	}

}
