package com.redislabs.recharge.batch;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.ProcessorConfiguration;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private Map<String, Expression> fields = new LinkedHashMap<>();
	private ProcessorConfiguration config;
	private Expression source;
	private StandardEvaluationContext context;

	public SpelProcessor(StatefulRediSearchConnection<String, String> connection, ProcessorConfiguration processor) {
		this.config = processor;
		this.source = processor.getSource() == null ? null : parser.parseExpression(processor.getSource());
		this.context = new StandardEvaluationContext();
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		context.setVariable("redis", connection.sync());
		config.getFields().forEach((k, v) -> fields.put(k, parser.parseExpression(v)));
	}

	@Override
	public Map<String, Object> process(Map<String, Object> record) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String, Object> map = source == null ? record : source.getValue(context, record, Map.class);
		fields.forEach((k, v) -> map.put(k, v.getValue(context, map)));
		return map;
	}

}
