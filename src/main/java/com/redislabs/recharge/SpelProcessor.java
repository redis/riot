package com.redislabs.recharge;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.ProcessorConfiguration;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SpelProcessor implements ItemProcessor<Map, Map>, StepExecutionListener {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private Map<String, Expression> fields = new LinkedHashMap<>();
	private ProcessorConfiguration config;
	private Expression source;
	private Expression merge;
	private StandardEvaluationContext context;
	private StatefulRediSearchConnection<String, String> connection;

	public SpelProcessor(StatefulRediSearchConnection<String, String> connection, ProcessorConfiguration processor) {
		this.connection = connection;
		this.config = processor;
		this.source = processor.getSource() == null ? null : parser.parseExpression(processor.getSource());
		this.merge = processor.getMerge() == null ? null : parser.parseExpression(processor.getMerge());
		this.context = new StandardEvaluationContext();
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		context.setVariable("redis", connection.sync());
		config.getFields().forEach((k, v) -> fields.put(k, parser.parseExpression(v)));
	}

	@Override
	public Map process(Map record) throws Exception {
		Map map = source == null ? record : source.getValue(context, record, Map.class);
		if (merge != null) {
			Map toMerge = merge.getValue(context, record, Map.class);
			if (toMerge != null) {
				map.putAll(toMerge);
			}
		}
		fields.forEach((k, v) -> map.put(k, v.getValue(context, map)));
		return map;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (connection != null) {
			connection.close();
		}
		return null;
	}

}
