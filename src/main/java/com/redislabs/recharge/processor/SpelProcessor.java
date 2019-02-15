package com.redislabs.recharge.processor;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.CachedRedis;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SpelProcessor implements ItemProcessor<Map, Map> {

	private String sourceExpression;
	private String mergeExpression;
	private Map<String, String> fields;
	private Expression source;
	private Expression merge;
	private StatefulRediSearchConnection<String, String> connection;
	private Map<String, Expression> fieldMap = new LinkedHashMap<>();
	private StandardEvaluationContext context;

	public StatefulRediSearchConnection<String, String> getConnection() {
		return connection;
	}

	public void setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	public String getMergeExpression() {
		return mergeExpression;
	}

	public void setMergeExpression(String mergeExpression) {
		this.mergeExpression = mergeExpression;
	}

	public Map<String, String> getFields() {
		return fields;
	}

	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}

	public String getSourceExpression() {
		return sourceExpression;
	}

	public void setSourceExpression(String sourceExpression) {
		this.sourceExpression = sourceExpression;
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
		for (Entry<String, Expression> entry : fieldMap.entrySet()) {
			Object value = entry.getValue().getValue(context, map);
			if (value != null) {
				map.put(entry.getKey(), value);
			}
		}
		return map;
	}

	public void open() throws Exception {
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
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		context.setVariable("r", connection.sync());
		context.setVariable("c", new CachedRedis(connection.sync()));
	}

	public void close() {
		context = null;
		fieldMap.clear();
		merge = null;
		source = null;
	}

}
