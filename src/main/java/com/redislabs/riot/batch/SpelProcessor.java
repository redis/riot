package com.redislabs.riot.batch;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionInvocationTargetException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(SpelProcessor.class);

	private SpelExpressionParser parser = new SpelExpressionParser();
	private StandardEvaluationContext context;
	private Map<String, Expression> expressions = new LinkedHashMap<>();

	public SpelProcessor(Object redis, DateFormat dateFormat, Map<String, String> variables,
			Map<String, String> fields) {
		context = new StandardEvaluationContext();
		context.setVariable("redis", redis);
		context.setVariable("date", dateFormat);
		variables.forEach((k, v) -> context.setVariable(k, parser.parseExpression(v).getValue(context)));
		Method geoMethod;
		try {
			geoMethod = getClass().getDeclaredMethod("geo", new Class[] { String.class, String.class });
			context.registerFunction("geo", geoMethod);
		} catch (NoSuchMethodException | SecurityException e) {
			log.error("Could not register geo function", e);
		}
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		fields.forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
	}

	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		for (Entry<String, Expression> entry : expressions.entrySet()) {
			try {
				Object value = entry.getValue().getValue(context, item);
				if (value == null) {
					continue;
				}
				item.put(entry.getKey(), value);
			} catch (ExpressionInvocationTargetException e) {
				log.error("Error while evaluating field {} with item {}", entry.getKey(), item, e);
			}
		}
		return item;
	}

	protected static String geo(String longitude, String latitude) {
		if (longitude == null || latitude == null) {
			return null;
		}
		return longitude + "," + latitude;
	}

}
