package com.redislabs.riot.processor;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>>, StepExecutionListener {

	private StatefulRediSearchConnection<String, String> connection;
	private String sourceExpression;
	private String mergeExpression;
	private Map<String, String> fields;
	private Expression source;
	private Expression merge;
	private Map<String, Expression> fieldMap = new LinkedHashMap<>();
	private StandardEvaluationContext context;

	public void setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	public void setMergeExpression(String mergeExpression) {
		this.mergeExpression = mergeExpression;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		SpelExpressionParser parser = new SpelExpressionParser();
		if (sourceExpression != null) {
			this.source = parser.parseExpression(sourceExpression);
		}
		if (mergeExpression != null) {
			this.merge = parser.parseExpression(mergeExpression);
		}
		if (fields != null) {
			fields.forEach((k, v) -> fieldMap.put(k, parser.parseExpression(v)));
		}
		this.context = new StandardEvaluationContext();
		this.context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		this.context.setVariable("r", connection.sync());
		this.context.setVariable("c", new CachedRedis(connection.sync()));
	}

	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}

	public void setSourceExpression(String sourceExpression) {
		this.sourceExpression = sourceExpression;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> process(Map<String, Object> record) throws Exception {
		Map<String, Object> map = source == null ? record : source.getValue(context, record, Map.class);
		if (merge != null) {
			Map<String, Object> toMerge = merge.getValue(context, record, Map.class);
			if (toMerge != null) {
				map.putAll(toMerge);
			}
		}
		for (Entry<String, Expression> entry : fieldMap.entrySet()) {
			Object value = entry.getValue().getValue(context, map);
			if (value != null) {
				map.put(entry.getKey(), value);
			}
		}
		return map;
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		context = null;
		fieldMap.clear();
		merge = null;
		source = null;
		return null;
	}

}
