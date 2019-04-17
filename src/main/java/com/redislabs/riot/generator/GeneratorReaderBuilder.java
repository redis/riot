package com.redislabs.riot.generator;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.lettusearch.RediSearchClient;

import lombok.Setter;

public class GeneratorReaderBuilder {

	private SpelExpressionParser parser = new SpelExpressionParser();

	@Setter
	private Map<String, String> fields;
	@Setter
	private Locale locale = Locale.ENGLISH;
	@Setter
	private RediSearchClient client;

	public GeneratorReader build() {
		Map<String, Expression> fieldExpressionMap = new LinkedHashMap<String, Expression>();
		fields.forEach((k, v) -> fieldExpressionMap.put(k, parser.parseExpression(v)));
		GeneratorReader reader = new GeneratorReader();
		reader.setFieldExpressions(fieldExpressionMap);
		reader.setLocale(locale);
		reader.setClient(client);
		return reader;
	}

}
