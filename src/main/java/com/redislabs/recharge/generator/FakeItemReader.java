package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

@Component
public class FakeItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, String>> {

	private ExpressionParser parser = new SpelExpressionParser();
	private EvaluationContext context;

	@Autowired
	private GeneratorConfiguration config;

	@PostConstruct
	public void open() {
		Faker faker = new Faker(new Locale(config.getLocale()));
		context = new StandardEvaluationContext(faker);
	}

	public FakeItemReader() {
		setName(ClassUtils.getShortName(FakeItemReader.class));
	}

	@Override
	protected Map<String, String> doRead() throws Exception {
		Map<String, String> map = new HashMap<>();
		for (Entry<String, String> entry : config.getFields().entrySet()) {
			Expression exp = parser.parseExpression(entry.getValue());
			Object value = exp.getValue(context);
			if (value != null) {
				map.put(entry.getKey(), String.valueOf(value));
			}
		}
		return map;
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
