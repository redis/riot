package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.Map;

import org.springframework.expression.EvaluationContext;

public class EmptyMapGenerator implements MapGenerator {

	@Override
	public Map<String, Object> generate(EvaluationContext evaluationContext) {
		return new HashMap<>();
	}

}
