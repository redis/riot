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

import lombok.Data;

@SuppressWarnings("rawtypes")
public class GeneratorEntityItemReader extends AbstractItemCountingItemStreamItemReader<Map> {

	private StatefulRediSearchConnection<String, String> connection;
	private GeneratorConfiguration config;
	private SpelExpressionParser parser = new SpelExpressionParser();
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private Expression map;
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private ThreadLocal<ReaderContext> readerContext = new ThreadLocal<>();

	@Data
	public static class ReaderContext {
		private int partitions;
		private int partitionIndex;
		private long count;

		public void incrementCount() {
			count++;
		}

		public long nextLong(long end) {
			return nextLong(0, end);
		}

		public long nextLong(long start, long end) {
			long segment = (end - start) / partitions;
			long partitionStart = start + partitionIndex * segment;
			return partitionStart + (count % segment);
		}

		public String nextId(long start, long end, String format) {
			return String.format(format, nextLong(start, end));
		}

		public String nextId(long end, String format) {
			return nextId(0, end, format);
		}
	}

	public GeneratorEntityItemReader(GeneratorConfiguration config,
			StatefulRediSearchConnection<String, String> connection) {
		setName("generator");
		this.config = config;
		this.connection = connection;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		ReaderContext readerContext = new ReaderContext();
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
		if (config.getMap() != null) {
			this.map = parser.parseExpression(config.getMap());
		}
		config.getFields().forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
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
		Map output = map == null ? new HashMap<>() : map.getValue(context.get(), Map.class);
		expressions.forEach((k, v) -> {
			Object value = v.getValue(context.get());
			output.put(k, value);
		});
		readerContext.get().incrementCount();
		return output;
	}

	@Override
	protected synchronized void doClose() throws Exception {
		context.remove();
	}

}
