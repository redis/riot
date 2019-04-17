package com.redislabs.riot.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;

import lombok.Setter;

public class ExpressionProcessor<I, O> implements ItemProcessor<I, O> {

	@Setter
	private Expression expression;
	@Setter
	private EvaluationContext context;
	private Class<? extends O> clazz;

	public ExpressionProcessor(Class<? extends O> clazz) {
		this.clazz = clazz;
	}

	@Override
	public O process(I item) throws Exception {
		return expression.getValue(context, item, clazz);
	}

}
