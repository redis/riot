package com.redislabs.recharge.batch;

import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.recharge.RechargeConfiguration.ProcessorConfiguration;

public class ExpressionSourcer implements Sourcer {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private Expression expression;

	public ExpressionSourcer(ProcessorConfiguration processor) {
		this.expression = parser.parseExpression(processor.getSource());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getSource(Map<String, Object> in) {
		return (Map<String, Object>) expression.getValue(new StandardEvaluationContext(in));
	}

}
