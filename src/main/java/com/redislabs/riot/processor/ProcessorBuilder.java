package com.redislabs.riot.processor;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class ProcessorBuilder {

	@Setter
	private String sourceExpression;
	@Setter
	private String mergeExpression;
	@Setter
	private Map<String, String> fieldExpressions = new LinkedHashMap<>();
	@Setter
	private StatefulRediSearchConnection<String, String> connection;

	public SpelProcessor build() {
		SpelProcessor processor = new SpelProcessor();
		StandardEvaluationContext context = new StandardEvaluationContext();
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		context.setVariable("r", connection.sync());
		context.setVariable("c", new CachedRedis(connection.sync()));
		SpelExpressionParser parser = new SpelExpressionParser();
		if (sourceExpression != null) {
			ExpressionProcessor<Map, Map> sourceProcessor = new ExpressionProcessor<Map, Map>(Map.class);
			sourceProcessor.setExpression(parser.parseExpression(sourceExpression));
			sourceProcessor.setContext(context);
			processor.setSourceProcessor(sourceProcessor);
		}
		if (mergeExpression != null) {
			MergeProcessor mergeProcessor = new MergeProcessor();
			mergeProcessor.setExpression(parser.parseExpression(mergeExpression));
			mergeProcessor.setContext(context);
			processor.setMergeProcessor(mergeProcessor);
		}
		if (!fieldExpressions.isEmpty()) {
			FieldProcessor fieldProcessor = new FieldProcessor();
			Map<String, Expression> expressions = new LinkedHashMap<>();
			fieldExpressions.forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
			fieldProcessor.setExpressions(expressions);
			fieldProcessor.setContext(context);
			processor.setFieldProcessor(fieldProcessor);
		}
		return processor;
	}

}