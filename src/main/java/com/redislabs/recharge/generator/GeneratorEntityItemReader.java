package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.recharge.RechargeConfiguration.GeneratorConfiguration;

public class GeneratorEntityItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private GeneratorConfiguration config;
	private EvaluationContext context;
	private ConversionService conversionService = new DefaultConversionService();
	private Map<String, Expression> expressions = new LinkedHashMap<>();

	public GeneratorEntityItemReader(GeneratorConfiguration config) {
		this.config = config;
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
	protected void doOpen() throws Exception {
		Faker faker = new Faker(new Locale(config.getLocale()));
		SpelExpressionParser parser = new SpelExpressionParser();
		this.context = new StandardEvaluationContext(faker);
		this.config.getFields().entrySet()
				.forEach(entry -> expressions.put(entry.getKey(), parser.parseExpression(entry.getValue())));
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
