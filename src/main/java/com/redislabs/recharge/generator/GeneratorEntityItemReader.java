package com.redislabs.recharge.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.ruaux.pojofaker.Faker;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.recharge.AbstractEntityItemReader;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.GeneratorEntityConfiguration;

public class GeneratorEntityItemReader extends AbstractEntityItemReader {

	private Faker faker;
	private ExpressionParser parser = new SpelExpressionParser();
	private EvaluationContext context;
	private ConversionService conversionService = new DefaultConversionService();
	private List<FieldExpression> expressions = new ArrayList<>();

	public GeneratorEntityItemReader(Entry<String, EntityConfiguration> entity) {
		super(entity);
		GeneratorEntityConfiguration config = entity.getValue().getGenerator();
		this.faker = new Faker(new Locale(config.getLocale()));
		this.context = new StandardEvaluationContext(faker);
		for (Entry<String, String> field : config.getFields().entrySet()) {
			FieldExpression fieldExpression = new FieldExpression();
			fieldExpression.setField(field.getKey());
			fieldExpression.setExpression(parser.parseExpression(field.getValue()));
			expressions.add(fieldExpression);
		}
	}

	@Override
	protected Map<String, Object> readValues() {
		Map<String, Object> map = new HashMap<>();
		for (FieldExpression fieldExpression : expressions) {
			Object source = fieldExpression.getExpression().getValue(context);
			if (source != null) {
				String value = conversionService.convert(source, String.class);
				map.put(fieldExpression.getField(), value);
			}
		}
		return map;
	}

}
