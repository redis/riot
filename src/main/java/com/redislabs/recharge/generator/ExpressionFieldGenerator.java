package com.redislabs.recharge.generator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import lombok.Setter;

@Setter
public class ExpressionFieldGenerator {

	private Map<String, Expression> expressions = new LinkedHashMap<>();

	public void generate(EvaluationContext evaluationContext, Map<String, Object> map) {
		expressions.forEach((k, v) -> map.put(k, v.getValue(evaluationContext)));
	}

}
