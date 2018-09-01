package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class GeneratorEntityItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private String locale;
	private Map<String, String> fields;
	private EvaluationContext context;
	private ConversionService conversionService = new DefaultConversionService();
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private StringRedisTemplate template;

	public GeneratorEntityItemReader(StringRedisTemplate template, String locale, Map<String, String> fields) {
		this.template = template;
		this.locale = locale;
		this.fields = fields;
	}

	@Override
	protected void doOpen() throws Exception {
		this.context = new StandardEvaluationContext(new RechargeFaker(template, new Locale(locale)));
		SpelExpressionParser parser = new SpelExpressionParser();
		this.fields.entrySet()
				.forEach(entry -> expressions.put(entry.getKey(), parser.parseExpression(entry.getValue())));
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		for (Entry<String, Expression> expression : expressions.entrySet()) {
			Object source = expression.getValue().getValue(context);
			if (source != null) {
				String value = conversionService.convert(source, String.class);
				map.put(expression.getKey(), value);
			}
		}
		return map;
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
