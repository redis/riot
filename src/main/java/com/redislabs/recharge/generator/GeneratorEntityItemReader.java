package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.IndexedPartitioner;
import com.redislabs.recharge.RechargeConfiguration.GeneratorConfiguration;

import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("rawtypes")
@Slf4j
public class GeneratorEntityItemReader extends AbstractItemCountingItemStreamItemReader<Map> {

	private StatefulRediSearchConnection<String, String> connection;
	private GeneratorConfiguration config;
	private SpelExpressionParser parser = new SpelExpressionParser();
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private Expression map;
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private ThreadLocal<Integer> partition = new ThreadLocal<>();

	public GeneratorEntityItemReader(GeneratorConfiguration config,
			StatefulRediSearchConnection<String, String> connection) {
		setName("generator");
		this.config = config;
		this.connection = connection;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partition.set(getPartition(executionContext));
		log.info("Partition index: {}", partition.get());
		super.open(executionContext);
	}

	private Integer getPartition(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_INDEX)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_INDEX);
		}
		return 0;
	}

	@Override
	protected void doOpen() throws Exception {
		if (config.getMap() != null) {
			this.map = parser.parseExpression(config.getMap());
		}
		config.getFields().forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
		RediSearchCommands<String, String> commands = connection.sync();
		StandardEvaluationContext context = new StandardEvaluationContext(new Faker(new Locale(config.getLocale())));
		context.setVariable("redis", commands);
		context.setVariable("sequence", new RedisSequence(commands));
		context.setVariable("partition", partition.get());
		this.context.set(context);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Map doRead() throws Exception {
		Map output = map == null ? new HashMap<>() : map.getValue(context.get(), Map.class);
		expressions.forEach((k, v) -> {
			output.put(k, v.getValue(context.get()));
		});
		return output;
	}

	@Override
	protected synchronized void doClose() throws Exception {
		context.remove();
	}

}
