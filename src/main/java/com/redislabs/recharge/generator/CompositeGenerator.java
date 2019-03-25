package com.redislabs.recharge.generator;

import java.util.Map;

import org.springframework.expression.EvaluationContext;

import lombok.Setter;

@Setter
public class CompositeGenerator implements MapGenerator {

	private MapGenerator mapGenerator;
	private ExpressionFieldGenerator fieldGenerator;

	@Override
	public Map<String, Object> generate(EvaluationContext evaluationContext) {
		Map<String, Object> map = mapGenerator.generate(evaluationContext);
		fieldGenerator.generate(evaluationContext, map);
		return map;
	}

}
