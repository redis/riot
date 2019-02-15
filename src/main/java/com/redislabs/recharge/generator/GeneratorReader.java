package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.IndexedPartitioner;

@SuppressWarnings("rawtypes")
public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map> implements InitializingBean {

	private volatile boolean initialized = false;
	private Object lock = new Object();
	private volatile int current = 0;
	private StatefulRediSearchConnection<String, String> connection;
	private String mapExpression;
	private Map<String, String> fields;
	private String locale;
	private StandardEvaluationContext evaluationContext;
	private Expression map;
	private Map<String, Expression> expressions;
	private int partitionIndex;
	private int partitions;

	public GeneratorReader() {
		setName(ClassUtils.getShortName(GeneratorReader.class));
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(fields, "Fields must not be null");
	}

	public void setMapExpression(String mapExpression) {
		this.mapExpression = mapExpression;
	}

	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public void setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partitionIndex = getPartitionIndex(executionContext);
		this.partitions = getPartitions(executionContext);
		super.open(executionContext);
	}

	private int getPartitionIndex(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_INDEX)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_INDEX);
		}
		return 0;
	}

	public int getPartitions() {
		return partitions;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public int getCurrent() {
		return current;
	}

	private int getPartitions(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_PARTITIONS)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_PARTITIONS);
		}
		return 1;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(!initialized, "Cannot open an already open ItemReader, call close first");
		SpelExpressionParser parser = new SpelExpressionParser();
		if (mapExpression != null) {
			this.map = parser.parseExpression(mapExpression);
		}
		this.expressions = new LinkedHashMap<>();
		for (Entry<String, String> field : fields.entrySet()) {
			expressions.put(field.getKey(), parser.parseExpression(field.getValue()));
		}
		Faker faker = locale == null ? new Faker() : new Faker(new Locale(locale));
		this.evaluationContext = new StandardEvaluationContext(faker);
		evaluationContext.setVariable("reader", this);
		if (connection != null) {
			evaluationContext.setVariable("redis", connection.sync());
			evaluationContext.setVariable("sequence", new RedisSequence(connection.sync()));
		}
		initialized = true;
	}

	public long nextLong(long end) {
		return nextLong(0, end);
	}

	public long nextLong(long start, long end) {
		long segment = (end - start) / partitions;
		long partitionStart = start + partitionIndex * segment;
		return partitionStart + (current % segment);
	}

	public String nextId(long start, long end, String format) {
		return String.format(format, nextLong(start, end));
	}

	public String nextId(long end, String format) {
		return nextId(0, end, format);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Map doRead() throws Exception {
		synchronized (lock) {
			Map output = map == null ? new HashMap<>() : map.getValue(evaluationContext, Map.class);
			for (Entry<String, Expression> expression : expressions.entrySet()) {
				Object value = expression.getValue().getValue(evaluationContext);
				if (value != null) {
					output.put(expression.getKey(), value);
				}
			}
			current++;
			return output;
		}
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			initialized = false;
			current = 0;
			evaluationContext = null;
			map = null;
		}
	}

}
