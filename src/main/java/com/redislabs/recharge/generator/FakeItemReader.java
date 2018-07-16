package com.redislabs.recharge.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ClassUtils;

public class FakeItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private ExpressionParser parser = new SpelExpressionParser();
	private EvaluationContext context;
	private ConversionService conversionService = new DefaultConversionService();
	private List<FieldExpression> expressions = new ArrayList<>();

	public FakeItemReader(Faker faker, Map<String, String> fields) {
		setName(ClassUtils.getShortName(FakeItemReader.class));
		this.context = new StandardEvaluationContext(faker);
		for (Entry<String, String> entry : fields.entrySet()) {
			FieldExpression fieldExpression = new FieldExpression();
			fieldExpression.setField(entry.getKey());
			fieldExpression.setExpression(parser.parseExpression(entry.getValue()));
			expressions.add(fieldExpression);
		}
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
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

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
