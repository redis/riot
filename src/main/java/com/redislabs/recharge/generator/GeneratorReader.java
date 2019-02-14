package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.AbstractCountingReader;
import com.redislabs.recharge.IndexedPartitioner;
import com.redislabs.recharge.RechargeConfiguration.GeneratorConfiguration;

@SuppressWarnings("rawtypes")
public class GeneratorReader extends AbstractCountingReader<Map> {

	private GeneratorConfiguration config;
	private StatefulRediSearchConnection<String, String> connection;
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private ThreadLocal<Expression> map;
	private ThreadLocal<Map<String, Expression>> expressions = new ThreadLocal<>();
	private ThreadLocal<GeneratorReaderContext> readerContext = new ThreadLocal<>();

	public GeneratorReader(GeneratorConfiguration config, StatefulRediSearchConnection<String, String> connection) {
		setName("generator");
		this.config = config;
		this.connection = connection;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		GeneratorReaderContext readerContext = new GeneratorReaderContext();
		readerContext.setPartitionIndex(getPartitionIndex(executionContext));
		readerContext.setPartitions(getPartitions(executionContext));
		this.readerContext.set(readerContext);
		super.open(executionContext);
	}

	private int getPartitionIndex(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_INDEX)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_INDEX);
		}
		return 0;
	}

	private int getPartitions(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_PARTITIONS)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_PARTITIONS);
		}
		return 1;
	}

	@Override
	protected void doOpen() throws Exception {
		SpelExpressionParser parser = new SpelExpressionParser();
		if (config.getMap() != null) {
			this.map = new ThreadLocal<>();
			this.map.set(parser.parseExpression(config.getMap()));
		}
		Map<String, Expression> expressionMap = new LinkedHashMap<>();
		config.getFields().forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
		expressions.set(expressionMap);
		RediSearchCommands<String, String> commands = connection.sync();
		StandardEvaluationContext context = new StandardEvaluationContext(new Faker(new Locale(config.getLocale())));
		context.setVariable("redis", commands);
		context.setVariable("sequence", new RedisSequence(commands));
		context.setVariable("context", readerContext.get());
		this.context.set(context);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Map doRead() throws Exception {
		Map output = map == null ? new HashMap<>() : map.get().getValue(context.get(), Map.class);
		for (Entry<String, Expression> expression : expressions.get().entrySet()) {
			Object value = expression.getValue().getValue(context.get());
			if (value != null) {
				output.put(expression.getKey(), value);
			}
		}
		readerContext.get().incrementCount();
		return output;
	}

	@Override
	protected void doClose() throws Exception {
		context.remove();
	}

}
