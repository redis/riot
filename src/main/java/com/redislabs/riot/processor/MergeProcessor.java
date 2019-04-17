package com.redislabs.riot.processor;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class MergeProcessor implements ItemProcessor<Map, Map> {

	@Setter
	private Expression expression;
	@Setter
	private EvaluationContext context;

	@SuppressWarnings("unchecked")
	public Map process(Map item) throws Exception {
		Map value = expression.getValue(context, item, Map.class);
		if (value != null) {
			item.putAll(value);
		}
		return value;
	}

}
