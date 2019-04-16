package com.redislabs.riot.generator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

import lombok.Setter;

public class GeneratorReaderBuilder {

	private SpelExpressionParser parser = new SpelExpressionParser();

	@Setter
	private String mapExpression;
	@Setter
	private Map<String, String> fieldExpressions;
	@Setter
	private String locale;
	@Setter
	private StatefulRediSearchConnection<String, String> connection;

	public GeneratorReader build() {
		CompositeGenerator generator = new CompositeGenerator();
		generator.setMapGenerator(mapGenerator(mapExpression));
		Map<String, Expression> fieldExpressionMap = new LinkedHashMap<String, Expression>();
		fieldExpressions.forEach((k, v) -> fieldExpressionMap.put(k, parser.parseExpression(v)));
		ExpressionFieldGenerator expressionFieldGenerator = new ExpressionFieldGenerator();
		expressionFieldGenerator.setExpressions(fieldExpressionMap);
		generator.setFieldGenerator(expressionFieldGenerator);
		GeneratorReader reader = new GeneratorReader();
		reader.setGenerator(generator);
		reader.setLocale(locale);
		reader.setConnection(connection);
		return reader;
	}

	private MapGenerator mapGenerator(String expression) {
		if (expression == null) {
			return new EmptyMapGenerator();
		}
		ExpressionMapGenerator generator = new ExpressionMapGenerator();
		generator.setExpression(parser.parseExpression(expression));
		return generator;
	}

}
