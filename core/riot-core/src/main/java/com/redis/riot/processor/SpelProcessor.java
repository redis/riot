package com.redis.riot.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.util.Assert;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private final EvaluationContext context;
	private final Map<String, Expression> expressions;
	private final AtomicLong index = new AtomicLong();

	public SpelProcessor(EvaluationContext context, Map<String, Expression> expressions) {
		Assert.notNull(context, "A SpEL evaluation context is required.");
		Assert.notEmpty(expressions, "At least one field is required.");
		this.context = context;
		this.expressions = expressions;
		this.context.setVariable("index", index);
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) {
		Map<String, Object> map = new HashMap<>(item);
		synchronized (context) {
			for (Entry<String, Expression> entry : expressions.entrySet()) {
				Object value = entry.getValue().getValue(context, map);
				if (value == null) {
					map.remove(entry.getKey());
				} else {
					map.put(entry.getKey(), value);
				}
			}
			index.incrementAndGet();
		}
		return map;
	}

}
