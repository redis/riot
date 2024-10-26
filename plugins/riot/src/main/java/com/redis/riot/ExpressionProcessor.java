package com.redis.riot;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.Expression;

public class ExpressionProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private final EvaluationContext context;
	private final Map<String, Expression> expressions;

	public ExpressionProcessor(EvaluationContext context, Map<String, Expression> expressions) {
		this.context = context;
		this.expressions = expressions;
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		expressions.forEach((k, v) -> item.put(k, v.getValue(context, item)));
		return item;
	}

}