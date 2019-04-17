package com.redislabs.riot.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class FieldProcessor implements ItemProcessor<Map, Map> {

	@Setter
	private EvaluationContext context;
	@Setter
	private Map<String, Expression> expressions = new LinkedHashMap<>();

	@SuppressWarnings("unchecked")
	public Map process(Map item) throws Exception {
		expressions.forEach((k, v) -> {
			Object value = v.getValue(context, item);
			if (value != null) {
				item.put(k, value);
			}
		});
		return item;
	}

}
