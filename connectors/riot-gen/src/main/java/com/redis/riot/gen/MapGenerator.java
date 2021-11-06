package com.redis.riot.gen;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

import com.github.javafaker.Faker;

import lombok.Builder;
import lombok.NonNull;

public class MapGenerator implements Generator<Map<String, Object>> {

	public static final String FIELD_INDEX = "index";

	private final SimpleEvaluationContext context;
	private final Map<String, Expression> expressions;

	@Builder
	private MapGenerator(Locale locale, @NonNull Map<String, String> fields) {
		Faker faker = new Faker(locale == null ? Locale.getDefault() : locale);
		this.context = new SimpleEvaluationContext.Builder(new ReflectivePropertyAccessor()).withInstanceMethods()
				.withRootObject(faker).build();
		SpelExpressionParser parser = new SpelExpressionParser();
		this.expressions = fields.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, e -> parser.parseExpression(e.getValue())));
	}

	@Override
	public Map<String, Object> next(long index) {
		Map<String, Object> map = new HashMap<>();
		context.setVariable(FIELD_INDEX, index);
		for (Entry<String, Expression> expression : expressions.entrySet()) {
			map.put(expression.getKey(), expression.getValue().getValue(context));
		}
		return map;
	}
}
