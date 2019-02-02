package com.redislabs.recharge.reader.generator;

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

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.IndexedPartitioner;
import com.redislabs.recharge.RechargeConfiguration.GeneratorReaderConfiguration;

import lombok.Builder;
import lombok.Data;

@SuppressWarnings("rawtypes")
public class GeneratorEntityItemReader extends AbstractItemCountingItemStreamItemReader<Map> {

	private String locale;
	private SpelExpressionParser parser = new SpelExpressionParser();
	private EvaluationContext context;
	private Expression map;
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private RediSearchClient client;
	private StatefulRediSearchConnection<String, String> connection;
	private Partition partition;

	@Data
	@Builder
	public static class Partition {
		private int index;
		private int size;
	}

	public GeneratorEntityItemReader(RediSearchClient client, GeneratorReaderConfiguration config) {
		this.client = client;
		this.locale = config.getLocale();
		if (config.getMap() != null) {
			this.map = parser.parseExpression(config.getMap());
		}
		config.getFields().forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		int partitionSize = executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_PARTITIONS)
				? executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_PARTITIONS)
				: 1;
		int partitionIndex = executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_INDEX)
				? executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_INDEX)
				: 0;
		this.partition = Partition.builder().size(partitionSize).index(partitionIndex).build();
		super.open(executionContext);
	}

	@Override
	protected void doOpen() throws Exception {
		connection = client.connect();
		context = new StandardEvaluationContext(new Faker(new Locale(locale)));
		context.setVariable("redis", connection.sync());
		context.setVariable("sequence", new RedisSequence(connection));
		context.setVariable("partition", partition);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Map doRead() throws Exception {
		Map output = map == null ? new HashMap<>() : map.getValue(context, Map.class);
		expressions.forEach((k, v) -> {
			output.put(k, v.getValue(context));
		});
		return output;
	}

	@Override
	protected synchronized void doClose() throws Exception {
		if (connection != null) {
			connection.close();
			connection = null;
		}
		context = null;
	}

}
