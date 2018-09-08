package com.redislabs.recharge.batch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private Map<String, String> fields;
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private StringRedisTemplate redis;

	public SpelProcessor(StringRedisTemplate redis, Map<String, String> fields) {
		this.redis = redis;
		this.fields = fields;
		this.fields.entrySet().forEach(f -> expressions.put(f.getKey(), parser.parseExpression(f.getValue())));
	}

	@Override
	public Map<String, Object> process(Map<String, Object> in) throws Exception {
		ItemContext rootObject = ItemContext.builder().redis(redis).in(in).build();
		StandardEvaluationContext context = new StandardEvaluationContext(rootObject);
		for (Entry<String, Expression> expression : expressions.entrySet()) {
			in.put(expression.getKey(), expression.getValue().getValue(context));
		}
		return in;
	}

}
