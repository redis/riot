package com.redislabs.riot;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class Processor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private StandardEvaluationContext context;
	private Map<String, Expression> expressions = new LinkedHashMap<>();

	public Processor(Map<String, String> fields) throws NoSuchMethodException, SecurityException {
		context = new StandardEvaluationContext();
		context.registerFunction("geo",
				getClass().getDeclaredMethod("geo", new Class[] { String.class, String.class }));
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		fields.forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
	}

	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		expressions.forEach((key, expression) -> item.put(key, expression.getValue(context, item)));
		return item;
	}

	public static String geo(String longitude, String latitude) {
		return longitude + "," + latitude;
	}

}
