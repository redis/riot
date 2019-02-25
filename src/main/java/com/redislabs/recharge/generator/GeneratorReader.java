package com.redislabs.recharge.generator;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

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
import com.redislabs.recharge.CachedRedis;
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
	private long sleep = 0;
	private int sleepNanos;

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

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	public void setSleepNanos(int sleepNanos) {
		this.sleepNanos = sleepNanos;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partitionIndex = IndexedPartitioner.getPartitionIndex(executionContext);
		this.partitions = IndexedPartitioner.getPartitions(executionContext);
		super.open(executionContext);
	}

	public int getPartitions() {
		return partitions;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public int current() {
		return current;
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
		GeneratorRootObject root = getRootObject();
		root.setReader(this);
		this.evaluationContext = new StandardEvaluationContext(root);
		if (connection != null) {
			evaluationContext.setVariable("r", connection.sync());
			evaluationContext.setVariable("c", new CachedRedis(connection.sync()));
		}
		initialized = true;
	}

	private GeneratorRootObject getRootObject() {
		if (locale == null) {
			return new GeneratorRootObject();
		}
		return new GeneratorRootObject(new Locale(locale));
	}

	public long current(long end) {
		return current / segment(end);
	}

	public long nextLong(long end) {
		return nextLong(0, end);
	}

	public long nextLong(long start, long end) {
		long segment = segment(start, end);
		long partitionStart = start + partitionIndex * segment;
		return partitionStart + (current % segment);
	}

	public long segment(long end) {
		return segment(0, end);
	}

	public long segment(long start, long end) {
		return (end - start) / partitions;
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
			Map output = new LinkedHashMap<>();
			if (map != null) {
				output.putAll(map.getValue(evaluationContext, Map.class));
			}
			for (Entry<String, Expression> expression : expressions.entrySet()) {
				Object value = expression.getValue().getValue(evaluationContext);
				if (value != null) {
					output.put(expression.getKey(), value);
				}
			}
			current++;
			if (sleep > 0) {
				Thread.sleep(sleep, sleepNanos);
			}
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
