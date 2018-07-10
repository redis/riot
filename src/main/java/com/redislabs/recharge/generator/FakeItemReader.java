package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ClassUtils;

public class FakeItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private ExpressionParser parser = new SpelExpressionParser();
	private EvaluationContext context;
	private Set<Entry<String, String>> fields;
	private ConversionService conversionService = new DefaultConversionService();

	public FakeItemReader(Faker faker, Set<Entry<String, String>> fields) {
		setName(ClassUtils.getShortName(FakeItemReader.class));
		this.context = new StandardEvaluationContext(faker);
		this.fields = fields;
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		for (Entry<String, String> entry : fields) {
			Expression exp = parser.parseExpression(entry.getValue());
			Object source = exp.getValue(context);
			if (source != null) {
				String value = conversionService.convert(source, String.class);
				map.put(entry.getKey(), value);
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
