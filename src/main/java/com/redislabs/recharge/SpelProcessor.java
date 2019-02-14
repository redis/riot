package com.redislabs.recharge;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.ProcessorConfiguration;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SpelProcessor implements ItemProcessor<Map, Map> {

	private SpelExpressionParser parser = new SpelExpressionParser();
	private Map<String, Expression> fields = new LinkedHashMap<>();
	private Expression source;
	private Expression merge;
	private StandardEvaluationContext context;

	public SpelProcessor(ProcessorConfiguration processor, StatefulRediSearchConnection<String, String> connection) {
		this.source = processor.getSource() == null ? null : parser.parseExpression(processor.getSource());
		this.merge = processor.getMerge() == null ? null : parser.parseExpression(processor.getMerge());
		processor.getFields().forEach((k, v) -> fields.put(k, parser.parseExpression(v)));
		this.context = new StandardEvaluationContext();
		this.context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		this.context.setVariable("redis", connection.sync());
	}

	@Override
	public Map process(Map record) throws Exception {
		Map map = source == null ? record : source.getValue(context, record, Map.class);
		if (merge != null) {
			Map toMerge = merge.getValue(context, record, Map.class);
			if (toMerge != null) {
				map.putAll(toMerge);
			}
		}
		for (Entry<String, Expression> entry : fields.entrySet()) {
			Object value = entry.getValue().getValue(context, map);
			if (value != null) {
				map.put(entry.getKey(), value);
			}
		}
		return map;
	}

}
