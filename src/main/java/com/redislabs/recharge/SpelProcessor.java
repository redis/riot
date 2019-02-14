package com.redislabs.recharge;

import java.util.Arrays;
import java.util.HashMap;
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

	private ProcessorConfiguration config;
	private StatefulRediSearchConnection<String, String> connection;
	private ThreadLocal<Map<String, Expression>> fields = new ThreadLocal();
	private ThreadLocal<Expression> source = new ThreadLocal<>();
	private ThreadLocal<Expression> merge = new ThreadLocal<>();
	private ThreadLocal<StandardEvaluationContext> context = new ThreadLocal<>();

	public SpelProcessor(ProcessorConfiguration config, StatefulRediSearchConnection<String, String> connection) {
		this.config = config;
		this.connection = connection;
	}

	@Override
	public Map process(Map record) throws Exception {
		Map map = source.get() == null ? record : source.get().getValue(context.get(), record, Map.class);
		if (merge.get() != null) {
			Map toMerge = merge.get().getValue(context.get(), record, Map.class);
			if (toMerge != null) {
				map.putAll(toMerge);
			}
		}
		for (Entry<String, Expression> entry : fields.get().entrySet()) {
			Object value = entry.getValue().getValue(context.get(), map);
			if (value != null) {
				map.put(entry.getKey(), value);
			}
		}
		return map;
	}

	public void open() throws Exception {
		SpelExpressionParser parser = new SpelExpressionParser();
		if (config.getSource() != null) {
			this.source.set(parser.parseExpression(config.getSource()));
		}
		if (config.getMerge() != null) {
			this.merge.set(parser.parseExpression(config.getMerge()));
		}
		this.fields.set(new HashMap<>());
		config.getFields().forEach((k, v) -> fields.get().put(k, parser.parseExpression(v)));
		this.context.set(new StandardEvaluationContext());
		context.get().setPropertyAccessors(Arrays.asList(new MapAccessor()));
		context.get().setVariable("redis", connection.sync());
		context.get().setVariable("cachedRedis", new CachedRedis(connection.sync()));
	}

	public void close() {
		context.remove();
		fields.remove();
		merge.remove();
		source.remove();
	}

}
