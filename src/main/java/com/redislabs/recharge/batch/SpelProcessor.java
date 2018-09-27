package com.redislabs.recharge.batch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.recharge.RechargeConfiguration.ProcessorConfiguration;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private Expression map;
	private Map<String, Expression> fields = new LinkedHashMap<>();
	private StringRedisConnection redis;
	private ProcessorConfiguration config;

	public SpelProcessor(StringRedisConnection redis, ProcessorConfiguration processor) {
		this.redis = redis;
		this.config = processor;
		if (processor.getPutAll() != null) {
			this.map = parser.parseExpression(processor.getPutAll());
		}
		config.getFields().entrySet().forEach(f -> fields.put(f.getKey(), parser.parseExpression(f.getValue())));
	}

	@Override
	public Map<String, Object> process(Map<String, Object> in) throws Exception {
		ItemContext rootObject = ItemContext.builder().redis(redis).in(in).build();
		StandardEvaluationContext context = new StandardEvaluationContext(rootObject);
		for (Entry<String, Expression> expression : fields.entrySet()) {
			Object value = expression.getValue().getValue(context);
			if (value == null) {
				continue;
			}
			in.put(expression.getKey(), value);
		}
		if (map != null) {
			Object value = map.getValue(context);
			if (value != null && value instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> valueMap = (Map<String, Object>) value;
				in.putAll(valueMap);
			}
		}
		return in;
	}

}
