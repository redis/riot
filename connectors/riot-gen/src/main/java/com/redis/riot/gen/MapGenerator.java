package com.redis.riot.gen;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

import com.github.javafaker.Faker;

public class MapGenerator implements Generator<Map<String, Object>> {

	public static final String FIELD_INDEX = "index";

	private final EvaluationContext context;
	private final Map<String, Expression> expressions;

	public MapGenerator(EvaluationContext context, Map<String, Expression> expressions) {
		this.context = context;
		this.expressions = expressions;
	}

	@Override
	public Map<String, Object> next(int index) {
		Map<String, Object> map = new HashMap<>();
		context.setVariable(FIELD_INDEX, index);
		for (Entry<String, Expression> expression : expressions.entrySet()) {
			map.put(expression.getKey(), expression.getValue().getValue(context));
		}
		return map;
	}

	public static MapGeneratorBuilder builder() {
		return new MapGeneratorBuilder();
	}

	public static class MapGeneratorBuilder {

		private final SpelExpressionParser parser = new SpelExpressionParser();
		private Locale locale = Locale.getDefault();
		private final Map<String, Expression> expressions = new LinkedHashMap<>();

		public MapGeneratorBuilder locale(Locale locale) {
			this.locale = locale;
			return this;
		}

		public MapGeneratorBuilder field(String field, String expression) {
			this.expressions.put(field, parser.parseExpression(expression));
			return this;
		}

		public MapGeneratorBuilder fields(Map<String, String> fields) {
			fields.forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
			return this;
		}

		public MapGenerator build() {
			Faker faker = new Faker(locale);
			SimpleEvaluationContext context = new SimpleEvaluationContext.Builder(new ReflectivePropertyAccessor())
					.withInstanceMethods().withRootObject(faker).build();
			return new MapGenerator(context, expressions);
		}

	}
}
