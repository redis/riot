package com.redislabs.riot.generator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeneratorConfig {

	private SpelExpressionParser parser = new SpelExpressionParser();

	@Bean
	@StepScope
	@ConditionalOnProperty("generator.max")
	public GeneratorReader generatorReader(GeneratorProperties props,
			StatefulRediSearchConnection<String, String> connection, int partitions) {
		log.info("Reading {}", props);
		GeneratorReader reader = new GeneratorReader();
		reader.setGenerator(generator(props));
		reader.setLocale(props.getLocale());
		reader.setConnection(connection);
		reader.setMaxItemCount(props.getMax() / partitions);
		return reader;
	}

	private MapGenerator generator(GeneratorProperties props) {
		CompositeGenerator generator = new CompositeGenerator();
		generator.setMapGenerator(mapGenerator(props));
		generator.setFieldGenerator(fieldGenerator(props));
		return generator;
	}

	private ExpressionFieldGenerator fieldGenerator(GeneratorProperties props) {
		Map<String, Expression> expressions = new LinkedHashMap<String, Expression>();
		props.getFields().forEach(f -> expressions.put(f.getName(), parser.parseExpression(f.getExpression())));
		ExpressionFieldGenerator generator = new ExpressionFieldGenerator();
		generator.setExpressions(expressions);
		return generator;
	}

	private MapGenerator mapGenerator(GeneratorProperties props) {
		if (props.getExpression() == null) {
			return new EmptyMapGenerator();
		}
		ExpressionMapGenerator generator = new ExpressionMapGenerator();
		generator.setExpression(parser.parseExpression(props.getExpression()));
		return generator;
	}

}
