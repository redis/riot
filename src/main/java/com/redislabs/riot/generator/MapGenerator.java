package com.redislabs.riot.generator;

import java.util.Map;

import org.springframework.expression.EvaluationContext;

public interface MapGenerator {

	Map<String, Object> generate(EvaluationContext evaluationContext);

}
